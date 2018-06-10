/* Routines for usage in test_mode */

use server::state::State;

#[derive(Deserialize)]
struct TestingObjectSpec {
    pub governors: Vec<String>,
    pub size: usize,
}

pub fn test_scheduler(state: &mut State) {
    for oref in state.updates.new_objects.clone() {
        let testing = oref.get().spec.user.get("_testing").cloned();
        if let Some(value) = testing {
            let c: TestingObjectSpec = ::serde_json::from_value(value).unwrap();
            oref.get_mut().info.size = Some(c.size);
            for governor_id in c.governors {
                debug!(
                    "Forcing object id={} to governor={} with fake size={}",
                    oref.get().spec.id,
                    governor_id,
                    c.size
                );
                let wref = state
                    .graph
                    .governors
                    .get(&governor_id.parse().unwrap())
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
