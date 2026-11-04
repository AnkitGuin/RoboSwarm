from robostream.generator.synthetic import GeneratorOptions, generate
from pathlib import Path

if __name__ == "__main__":
    print(generate(Path("data/generated/standalone"), GeneratorOptions()))
