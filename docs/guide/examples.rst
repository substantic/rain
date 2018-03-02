
Examples
********


Distributed cross-validation with libsvm
========================================

::

    # =======================================================
    # This example creates a simple cross-validation pipeline
    # for libsvm tools over IRIS data set
    #
    # Requirements:
    # 1) Installed svm-train and svm-predict
    #    (libsvm-tools package on Debian)
    # 2) IRIS data set in CSV format, e.g.:
    #    https://raw.githubusercontent.com/pandas-dev/pandas/master/pandas/tests/data/iris.csv
    # =======================================================

    import os
    from rain.client import Client, tasks, Program, Input, Output, remote

    THIS_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_FILE = os.path.join(THIS_DIR, "iris.csv")
    CHUNKS = 3


    # Convert .csv to libsvm format
    @remote()
    def convert_to_libsvm_format(ctx, data):
        lines = [line.split(",") for line in data.get_str().rstrip().split("\n")]
        lines = lines[1:]  # Skip header
        labels = sorted(set(line[-1] for line in lines))

        result = "\n".join("{} 1:{} 2:{} 3:{} 4:{}".format(
            labels.index(line[4]),
            line[0], line[1], line[2], line[3]) for line in lines)
        return result


    def main():

        # Program: SVM train
        # svm-train has following usage: svm-train <trainset> <model>
        # It reads <trainset> and creates file <model> with trained model
        train = Program(("svm-train", Input("data"), Output("output")))

        # Porgram: SVM predict
        # svm-predict has following usage: svm-predict <testdata> <model> <prediction>
        # It reads files <testdata> and <model> and creates file with prediction and
        # prints accuracy on standard output
        predict = Program(("svm-predict", Input("testdata"), Input("model"), Output("prediction")),
                        stdout=Output("accuracy"))

        # Connect to rain server
        client = Client("localhost", 7210)
        with client.new_session() as session:

            # Load data - this is already task, so load is performed on worker
            input_data = tasks.open(DATA_FILE)

            # Convert data - note that the function is marked @remote
            # so it is not executed now, but on a worker
            converted_data = convert_to_libsvm_format(input_data)

            # Using unix command "sort" to shuffle dataset
            randomized_data = tasks.execute(("sort", "--random-sort", converted_data), stdout=True)

            # Create chunks via unix command "split"
            chunks = tasks.execute(("split", "-d", "-n", "l/{}".format(CHUNKS), randomized_data),
                                output_files=["x{:02}".format(i) for i in range(CHUNKS)]).outputs
                                # Note that we are taking "outputs" of the task here ==> ^^^^^^^^

            # Make folds
            train_sets = [tasks.concat(chunks[:i] + chunks[i+1:]) for i, c in enumerate(chunks)]

            # Train models
            models = [train(data=train_set) for train_set in train_sets]

            # Compute predictions
            predictions = [predict(model=model, testdata=data) for model, data in zip(models, chunks)]

            # Set "keep" flag for "accuracy" output on predictions
            for p in predictions:
                p.outputs["accuracy"].keep()

            # Submit and wait until everything is not completed
            session.submit()
            session.wait_all()

            # Print predictions
            for p in predictions:
                print(p.outputs["accuracy"].fetch().get_str())


    if __name__ == "__main__":
        main()
