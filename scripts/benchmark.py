from robostream.cli import main

if __name__ == "__main__":
    main(["benchmark", *__import__("sys").argv[1:]])
