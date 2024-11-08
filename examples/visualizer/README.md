# Visualizer

An example Chai application that displays a graphical representation of a specific
package.

## Requirements

1. [python]: version 3.11
2. [pip]: Ensure you have pip installed
3. [virtualenv]: It's recommended to use a virtual environment to manage dependencies

## Getting Started

1. Set up a virtual environment

```sh
python -m venv venv
source venv/bin/activate
```

2. Install required packages

```sh
pip install -r requirements.txt
```

3. Ensure `CHAI_DATABASE_URL` is available as an environment variable. The default
   value from our docker config is below:

```sh
export CHAI_DATABASE_URL=postgresql://postgres:s3cr3t@localhost:5432/chai
```

## Usage

1. Start the [Chai DB](https://github.com/teaxyz/chai-oss) with `docker compose up`.
1. Run the visualizer:
   ```sh
   python main.py <package>
   ```

### Arguments

- `--depth`: Maximum depth to go to. Default is `9999`, meaning all possible depths
- `--profile`: Enable performance profiling. Default is `False`.

## Share your visuals

If you create interesting visuals, share them on our [Discord]. Feel free to mess
around and create alternate ways to generate them.

[python]: https://www.python.org
[pip]: https://pip.pypa.io/en/stable/installation/
[virtualenv]: https://virtualenv.pypa.io/en/latest/
[Discord]: https://discord.com/invite/tea-906608167901876256
