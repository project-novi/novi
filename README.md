<p align="center">
  <img width="120" height="120" src="https://github.com/project-novi.png">
</p>
<p align="center"><b>Novi</b>: A <ins>no</ins>vel way to na<ins>vi</ins>gate anything.</p>

> [!WARNING]
> Novi is still in the very early stage of development, and is not suitable for daily use yet. For now, you may be interested in our [concepts](https://novi-docs.mivik.moe/concepts/). Also, contribution is welcome!

Novi is hard to define. It could be a blog, a image collection, a music player, a note taking app… you name it. To grasp the gist, Novi is a place of "objects". Objects can be anything (image, music, video), and you can tag them so you can navigate through them easily.

Novi comes with a comprehensive plugin system. You can write plugins in Python. You can query objects or subscribe to updates with filters. You can register functions to interact with other plugins, as well as the web interface. The plugin system is so powerful that most of the core functionalities are implemented as plugins: HTTP API, thumbnails generation, image optimization, etc.

The goals of Novi are:

- **Simple**: APIs should be minimal. The web interface should be clean and easy to use.
- **Extensible**: Achieve anything with plugins. Sky's the limit.
- **Performant**: Powered by Rust and backed by PostgreSQL, Novi is fast and reliable.
- **Safe**: A fine-grained permission system controls who can do what, down to the per-object level.

## Documentation

See <https://novi-docs.mivik.moe>.

## Dependencies

- PostgreSQL 14+
- Rust 1.75.0+
