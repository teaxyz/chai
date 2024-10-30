# SBOM-Meta

An example Chai application that displays a graphical representation of the dependencies
of a specific package.

## Installation

1. Start the [Chai DB](https://github.com/teaxyz/chai-oss) with `docker compose up`.
2. `pip install -r requirements.txt`
3. `python main.py --package <package>`

## Usage

Run `python main.py --package <package>` with CHAI running to get a graphviz
generated image of the dependency graph of that package.
