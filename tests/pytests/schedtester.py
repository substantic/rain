from rain.client import blob, remote, Resources


class Worker:

    def __init__(self, cpus):
        self.cpus = cpus
        self.worker_id = None


class Scenario:

    def __init__(self, test_env, workers):
        assert all(w.worker_id is None for w in workers)
        self.workers = tuple(workers)
        self.task_expected_placement = {}

        test_env.start(worker_defs=[w.cpus for w in workers])
        self.client = test_env.client

        ws = list(self.workers)
        for worker_info in self.client.get_server_info()["workers"]:
            cpus = int(worker_info["resources"]["cpus"])

            for w in ws:
                if w.cpus == cpus:
                    break
            else:
                raise Exception("Requested worker not found")
            ws.remove(w)
            w.worker_id = worker_info["worker_id"]
        assert not ws

        self.session = self.client.new_session()

    def new_object(self, workers, size):
        if isinstance(workers, Worker):
            workers = (workers,)
        assert all(w.worker_id for w in workers)
        with self.session.bind_only():
            obj = blob(b"")
            obj.attributes["__test"] = {
                "workers": [w.worker_id for w in workers],
                "size": size
            }
        return obj

    # TODO: Configurable size of output, now output has zero size
    def new_task(self, inputs, cpus=1, expect_worker=None):
        with self.session.bind_only():
            task = testing_task(inputs)
            if cpus != 1:
                task.resources = Resources(n_cpus=cpus)
            if expect_worker:
                if isinstance(expect_worker, Worker):
                    expect_worker = (expect_worker,)
                self.task_expected_placement[task] = expect_worker
            return task

    def run(self):
        with self.session:
            self.session.submit()
            self.session.wait_all()
            self.session.update(list(self.task_expected_placement))
            error = False
            for task, expected_workers in self.task_expected_placement.items():
                placement = task.attributes["info"]["worker"]
                if placement not in [w.worker_id for w in expected_workers]:
                    print("Task: ",
                          task.id_pair,
                          "was computed on",
                          placement,
                          "but expected on",
                          [w.worker_id for w in expected_workers])
                    error = True
            if error:
                raise Exception("Scenario failed, see stdout for more details")


@remote()
def testing_task(ctx, *args):
    return b""