Examples & Tutorials
********************

External links
==============

  * Tensorflow MNIST & Rain
     - Part 1 https://substantic.github.io/2018/09/05/hsearch1.html
     - Part 2 https://substantic.github.io/2018/09/11/hsearch2.html


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
        train = Program(("svm-train", Input("data"), Output("output")), name="train")

        # Porgram: SVM predict
        # svm-predict has following usage: svm-predict <testdata> <model> <prediction>
        # It reads files <testdata> and <model> and creates file with prediction and
        # prints accuracy on standard output
        predict = Program(("svm-predict", Input("testdata"), Input("model"), Output("prediction")),
                        stdout=Output("accuracy"), name="predict")

        # Connect to rain server
        client = Client("localhost", 7210)
        with client.new_session("SVM test") as session:

            # Load data - this is already task, so load is performed on governor
            input_data = tasks.Load(DATA_FILE, name="prepare/load")

            # Convert data - note that the function is marked @remote
            # so it is not executed now, but on a governor
            converted_data = convert_to_libsvm_format(input_data,
                                                    name="prepare/convert")

            # Using unix command "sort" to shuffle dataset
            randomized_data = tasks.Execute(("sort", "--random-sort", converted_data),
                                            stdout=True, name="prepare/randomize")

            # Create chunks via unix command "split"
            chunks = tasks.Execute(("split", "-d", "-n", "l/{}".format(CHUNKS), randomized_data),
                                output_paths=["x{:02}".format(i) for i in range(CHUNKS)],
                                name="prepare/split").outputs
            #                                           ^^^^^^^^ Note that we are taking "outputs"
            #                                           |||||||| of the task here

            # Make folds
            train_sets = [tasks.Concat(chunks[:i] + chunks[i+1:], name="prepare/concat")
                        for i, c in enumerate(chunks)]

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