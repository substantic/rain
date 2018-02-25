/* Routines for usage in test_mode */

use server::state::State;

#[derive(Debug, Deserialize)]
struct TestConfig {
    workers: Vec<String>,
    size: usize,
}

pub fn test_scheduler(state: &mut State) {
    for oref in state.updates.new_objects.clone() {
        let config: Option<TestConfig> = oref.get().attributes.find("__test").unwrap();
        if let Some(c) = config {
            oref.get_mut().size = Some(c.size);

            for worker_id in c.workers {
                debug!(
                    "Forcing object id={} to worker={} with fake size={}",
                    oref.get_mut().id,
                    worker_id,
                    c.size
                );
                let wref = state
                    .graph
                    .workers
                    .get(&worker_id.parse().unwrap())
                    .unwrap()
                    .clone();
                wref.get_mut().scheduled_objects.insert(oref.clone());
                oref.get_mut().scheduled.insert(wref.clone());
                state.update_object_assignments(&oref, Some(&wref));
                state.updates.new_objects.remove(&oref);
            }
        }
    }
}
