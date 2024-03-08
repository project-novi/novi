use crate::Result;
use image::DynamicImage;
use ndarray::{Array1, Array2, Array3, Axis};
use ort::{CPUExecutionProvider, GraphOptimizationLevel, Session};
use safetensors::{tensor::TensorView, SafeTensors};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    path::Path,
};
use tracing::warn;

#[derive(Serialize)]
pub struct InferredTag {
    pub tag: String,
    pub confidence: f64,
}

pub struct Image2Vec(Session);

impl Image2Vec {
    pub fn new(model: impl AsRef<Path>) -> ort::Result<Self> {
        let session = Session::builder()?
            .with_optimization_level(GraphOptimizationLevel::Level3)?
            .with_intra_threads(4)?
            .with_execution_providers([CPUExecutionProvider::default().build()])?
            .with_model_from_file(model)?;
        Ok(Self(session))
    }

    pub fn embedding(&self, img: DynamicImage) -> ort::Result<Array1<f32>> {
        let img = img.resize_exact(224, 224, image::imageops::FilterType::Triangle);
        let img = img.to_rgb8();
        let data = img.into_raw();
        let data = Array3::from_shape_vec((224, 224, 3), data)
            .unwrap()
            .permuted_axes([2, 0, 1])
            .map(|it| *it as f32 / 255.);
        let data = data.insert_axis(Axis(0));

        let outputs = self.0.run(ort::inputs!["input" => data]?)?;

        Ok(outputs["output"]
            .extract_tensor::<f32>()?
            .view()
            .t()
            .into_iter()
            .copied()
            .collect())
    }
}

struct ClassifyHead {
    weight: Array2<f32>,
    bias: Array1<f32>,
    tags: Vec<String>,
}
impl ClassifyHead {
    fn to_vec(t: &TensorView) -> Vec<f32> {
        t.data()
            .chunks_exact(4)
            .map(|it| f32::from_le_bytes(it.try_into().unwrap()))
            .collect()
    }

    fn new(file: impl AsRef<Path>, tags: Vec<String>) -> Self {
        let data = std::fs::read(file).expect("failed to read file");
        let st = SafeTensors::deserialize(&data).expect("invalid safetensors");

        let weight = st.tensor("weight").unwrap();
        let shape = weight.shape();
        let weight = Array1::from(Self::to_vec(&weight))
            .into_shape((shape[0], shape[1]))
            .unwrap();
        let bias = Self::to_vec(&st.tensor("bias").unwrap());

        Self {
            weight,
            bias: bias.into(),
            tags,
        }
    }

    fn raw_output(&self, input: &Array1<f32>) -> Array1<f32> {
        self.weight.dot(input) + &self.bias
    }

    fn classify(&self, input: &Array1<f32>, threshold: f32) -> Vec<InferredTag> {
        let output = self.raw_output(input);

        let mut tops = Vec::new();
        for (tag, prob) in self.tags.iter().zip(output.into_iter()) {
            if prob > threshold {
                tops.push(InferredTag {
                    tag: tag.clone(),
                    confidence: prob as f64,
                });
            }
        }

        tops.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());

        tops
    }
}

pub struct ImageModel {
    image2vec: Image2Vec,
    novi_class_head: ClassifyHead,
    dbr_class_head: ClassifyHead,
    dbr_tr_class_head: ClassifyHead,

    checksum: Vec<u8>,
}

impl ImageModel {
    fn read_tags(path: impl AsRef<Path>) -> Vec<String> {
        serde_json::from_reader(std::fs::File::open(path).expect("failed to read tags"))
            .expect("invalid tags")
    }

    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();

        let model = dir.join("model.onnx");
        let checksum = {
            let mut hasher = Sha256::new();
            let mut f = std::fs::File::open(&model).expect("failed to read file");
            std::io::copy(&mut f, &mut hasher).expect("failed to calculate hash");
            hasher.finalize().to_vec()
        };
        let image2vec = Image2Vec::new(model).expect("failed to load model");

        let novi_tags = Self::read_tags(dir.join("novi_tags.json"));
        let dbr_tags = Self::read_tags(dir.join("tags.json"));

        let novi_class_head = ClassifyHead::new(dir.join("novi-class.safetensors"), novi_tags);
        let dbr_class_head = ClassifyHead::new(dir.join("dbr-class.safetensors"), dbr_tags);

        let dbr_index: HashMap<&str, usize> = dbr_class_head
            .tags
            .iter()
            .enumerate()
            .map(|(i, it)| (it.as_str(), i))
            .collect();
        let dbr_tr: HashMap<String, String> = serde_json::from_reader(
            File::open(dir.join("dbr_translation.json")).expect("failed to read translation"),
        )
        .expect("invalid translation");
        let mut tensors = Vec::with_capacity(dbr_tr.len());
        let mut tensors_bias = Vec::with_capacity(dbr_tr.len());
        let mut dbr_tr_tags = Vec::new();
        let mut not_found = Vec::new();
        for (dbr, tag) in dbr_tr {
            let row = match dbr_index.get(dbr.as_str()) {
                Some(row) => *row,
                None => {
                    not_found.push(dbr);
                    continue;
                }
            };
            tensors.push(dbr_class_head.weight.index_axis(Axis(0), row));
            tensors_bias.push(dbr_class_head.bias[row]);
            dbr_tr_tags.push(tag.clone());
        }
        if !not_found.is_empty() {
            warn!(tags = ?not_found, "not found");
        }

        let dbr_tr_class_head = ClassifyHead {
            weight: ndarray::stack(Axis(0), &tensors).unwrap(),
            bias: tensors_bias.into(),
            tags: dbr_tr_tags,
        };

        Ok(Self {
            image2vec,
            novi_class_head,
            dbr_class_head,
            dbr_tr_class_head,

            checksum,
        })
    }

    pub fn checksum(&self) -> &[u8] {
        &self.checksum
    }

    pub fn predict(&self, embedding: Array1<f32>) -> Vec<InferredTag> {
        let mut tops = self.dbr_tr_class_head.classify(&embedding, 0.2);
        let existed = tops
            .iter()
            .map(|it| it.tag.clone())
            .collect::<BTreeSet<_>>();

        let my = self.novi_class_head.classify(&embedding, 0.13);
        for tag in my {
            if !tag.tag.starts_with("rating:") && !existed.contains(&tag.tag) {
                tops.push(tag);
            }
        }

        tops.sort_by(|a, b| b.confidence.partial_cmp(&a.confidence).unwrap());

        tops
    }

    pub fn predict_dbr(&self, embedding: Array1<f32>) -> Vec<InferredTag> {
        self.dbr_class_head.classify(&embedding, 0.2)
    }

    pub fn infer(&self, tags: &[&str]) -> Result<Vec<InferredTag>> {
        let mut tags = tags.iter().cloned().collect::<BTreeSet<_>>();

        let mut source = Vec::new();
        let mut candidates = Vec::new();
        let w = &self.novi_class_head.weight;
        for (tag, row) in self.novi_class_head.tags.iter().zip(w.rows()) {
            if tags.remove(tag.as_str()) {
                source.push(row);
            } else {
                candidates.push((tag, row));
            }
        }

        let mut candidates = candidates
            .into_iter()
            .map(|(tag, row)| {
                let mut score = 0.;
                for src in &source {
                    score += (src - &row).map(|it| it.powf(2.)).sum();
                }
                (tag, row, score)
            })
            .collect::<Vec<_>>();

        candidates.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap());
        candidates.retain(|it| !it.0.starts_with("rating:"));
        candidates.truncate(10);

        let res = candidates
            .into_iter()
            .map(|it| InferredTag {
                tag: it.0.clone(),
                confidence: 1. / it.2 as f64,
            })
            .collect();

        Ok(res)
    }

    pub fn embedding(&self, image: DynamicImage) -> Result<Vec<f32>> {
        let embedding = self.image2vec.embedding(image)?;

        Ok(embedding.into_iter().collect())
    }
}
