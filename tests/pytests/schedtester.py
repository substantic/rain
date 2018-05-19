from rain.client import blob, remote


class Governor:

    def __init__(self, cpus):
        self.cpus = cpus
        self.governor_id = None


class Scenario:

    def __init__(self, test_env, governors):
        assert all(w.governor_id is None for w in governors)
        self.governors = tuple(governors)
        self.task_expected_placement = {}

        test_env.start(governor_defs=[w.cpus for w in governors])
        self.client = test_env.client

        ws = list(self.governors)
        for i, governor_info in enumerate(self.client.get_server_info()["governors"]):
            cpus = int(governor_info["resources"]["cpus"])

            for w in ws:
                if w.cpus == cpus:
                    break
            else:
                raise Exception("Requested governor not found")
            ws.remove(w)
            w.governor_id = governor_info["governor_id"]
        assert not ws

        self.session = self.client.new_session()

    def new_object(self, governors, size):
        if isinstance(governors, Governor):
            governors = (governors,)
        assert all(w.governor_id for w in governors)
        with self.session.bind_only():
            obj = blob(b"")
            obj.attributes["__test"] = {
                "governors": [w.governor_id for w in governors],
                "size": size
            }
        return obj

    # TODO: Configurable size of output, now output has zero size
    def new_task(self, inputs, cpus=1, expect_governor=None, label=None):
        with self.session.bind_only():
            task = testing_task(inputs)
            task.test_label = label
            print("Creating task {} as {}".format(label, task))
            if cpus != 1:
                task.attributes["resources"]["cpus"] = cpus
            if expect_governor:
                if isinstance(expect_governor, Governor):
                    expect_governor = (expect_governor,)
                self.task_expected_placement[task] = expect_governor
            return task

    def run(self):
        with self.session:
            self.session.submit()
            self.session.wait_all()
            self.session.update(list(self.task_expected_placement))
            error = False
            for task, expected_governors in self.task_expected_placement.items():
                placement = task.attributes["info"]["governor"]
                print("Task {} computed on {}".format(task.test_label, placement))
                if placement not in [w.governor_id for w in expected_governors]:
                    print("!!! Task: ",
                          task.id,
                          "was computed on",
                          placement,
                          "but expected on",
                          [w.governor_id for w in expected_governors])
                    error = True
            if error:
                raise Exception("Scenario failed, see stdout for more details")


@remote()
def testing_task(ctx, *args):
    return b""
