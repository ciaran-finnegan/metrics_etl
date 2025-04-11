from core.pipeline import ETLEngine
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/signals.yaml")
    args = parser.parse_args()
    
    engine = ETLEngine(args.config)
    engine.run()