use std::collections::{
    HashMap, BTreeMap
};
use log::info;
use uuid::Uuid;

// round input
#[derive(Debug, Clone)]
pub struct Input {
    pub owner_peer_id: Option<Vec<u8>>,

    pub hash: u128
}

#[derive(Debug)]
pub struct Round {
    pub number: usize,
    
    // <index, input>
    pub inputs: BTreeMap<usize, Input>,
    
    // inputs are grouped into batches of batch size
    pub batch_size: usize,
    // helper for out of order feed arrival handling
    pub partial_batches: BTreeMap<usize, Vec<usize>>,
    // <batch index, batch>
    pub batches: BTreeMap<usize, Vec<usize>>,
    // unique id for the sub-job, params: <batch index, hash>
    pub batch_ids: HashMap<u128, usize>,
    pub inverse_batch_ids: HashMap<usize, u128>,

    // prover p1 has been assigned batch b
    // <batch index, provers>
    pub assigned_batches: HashMap<usize, Vec<Vec<u8>>>,
    // helper(^) reverse map to complement "assigned_batches": <prover, assigned batches>
    pub prover_batch_assignements: HashMap<Vec<u8>, Vec<usize>>,

    // <batch index, <prover, proof>>
    pub proofs: BTreeMap<usize, HashMap<Vec<u8>, Input>>,
    // helper(^) reverse map to complement "proofs": <prover, batches>
    pub prover_proofs: HashMap<Vec<u8>, Vec<usize>>,
}

impl Round {
    pub fn new(number: usize, batch_size: usize) -> Self {
        Self {
            number: number,
            inputs: BTreeMap::new(),
            batch_size: batch_size,
            partial_batches: BTreeMap::new(),            
            batches: BTreeMap::new(),
            batch_ids: HashMap::new(),
            inverse_batch_ids: HashMap::new(),
            assigned_batches: HashMap::new(),
            prover_batch_assignements: HashMap::new(),
            proofs: BTreeMap::new(),
            prover_proofs: HashMap::new(),
        }
    }

    // append the input to the partial batch and upgrade it needful
    pub fn feed_input(&mut self, index: usize, input: Input) {
        if self.inputs.contains_key(&index) {
            return
        }
        // info!("New input with index `{index}` has arrived.");
        self.inputs.insert(index, input);
        //@ handle 0th proof in join
        let batch_index = index / self.batch_size;
        self.partial_batches.entry(batch_index)
            .and_modify(|batch| {
                batch[index % self.batch_size] = index;
            })
            .or_insert_with(|| {
                let mut new_batch = vec![0usize; self.batch_size];
                new_batch[index % self.batch_size] = index;
                new_batch
            })
            ;
        // upgrade if batch is full
        if self.partial_batches
            .get(&batch_index)
            .unwrap()
            .iter()
            .skip(if batch_index == 0 { 1 } else { 0 })
            .all(|bi| *bi != 0usize)
        {
            if batch_index != 0 || (batch_index == 0 && self.inputs.contains_key(&0)) {
                self.batches.insert(
                    batch_index,
                    self.partial_batches.remove(&batch_index).unwrap()
                );
                let batch_id = Uuid::new_v4().as_u128();
                self.batch_ids.insert(batch_id, batch_index);
                self.inverse_batch_ids.insert(batch_index, batch_id);
            }
        }
    }

    // no new input is expected, so upgrade the partial batch into a full batch
    pub fn stop_feeding(&mut self) {
        if self.partial_batches.is_empty() {
            return
        }
        let (index, batch) = self.partial_batches.first_key_value().unwrap();
        let last_batch: Vec<_> = batch
            .iter()
            .filter_map(|item| if *item != 0 { Some(*item) } else { None })
            .collect();
        if last_batch.len() == 1 {
            // add it to the last batch
            self.batches
                .values_mut()
                .last()
                .unwrap()
                .push(last_batch.into_iter().next().unwrap());
        } else {
            self.batches.insert(*index, last_batch);
            let batch_id = Uuid::new_v4().as_u128();
            self.batch_ids.insert(batch_id, *index);
            self.inverse_batch_ids.insert(*index, batch_id);            
        }
        info!(
            "No more feeds for round `{}`, inputs: `{}`, batches: `{}`",
            self.number,
            self.inputs.len(),
            self.batches.len()
        );
    }

    // assign a batch to prover
    pub fn assign_batch(&self, prover: &Vec<u8>) -> Option<(u128, Vec<Input>)> {        
        let outstanding_batches: Vec<_> = self.batches.keys()
            .filter(|index| {
                // filter already proved batches
                !self.proofs.contains_key(&index) &&
                // and, if the batch is already assigned to this prover
                !self.assigned_batches.get(&index)
                    .and_then(|assignees| 
                        assignees
                            .iter()
                            .find(|p| *p == prover)
                    )
                .is_some()
            })
            .collect();
        if outstanding_batches.is_empty() {
            // info!("round: {}, b: {:?}", self.number, self.batches);
            return None
        }
        let (pick_closest, other_batches) = if self.prover_proofs.contains_key(prover) {
            // rank batches based on their distance to the proofs and pick the closest
            (true, self.prover_proofs.get(prover).unwrap().clone())
        } else {
            if self.prover_batch_assignements.contains_key(prover) {
                // rank batches based on their distance to the assigned batches and pick the closest
                (true, self.prover_batch_assignements.get(prover).unwrap().clone())
            }
            else {
                // brand new prover
                if !self.proofs.is_empty() {
                    (false, self.proofs.keys().copied().collect())
                } else {
                    (false, self.assigned_batches.keys().copied().collect())
                }
            }
        };
        let selected_batch_index = if !other_batches.is_empty() {
            let batch_distances = outstanding_batches
                .iter()
                .map(|batch_index| {
                    let mut dist = batch_index.abs_diff(other_batches[0]);
                    for other_batch_index in other_batches.iter().skip(1) {
                        let abs_diff = batch_index.abs_diff(*other_batch_index);
                        if abs_diff < dist {
                            dist = abs_diff;
                        }
                    }
                    (batch_index, dist)
                });
            let closest = if pick_closest {
                batch_distances.min_by(|x, y| x.1.cmp(&y.1))
            } else {
                batch_distances.max_by(|x, y| x.1.cmp(&y.1))
            }
            .unwrap()
            .0;
            **closest
        } else {
            0
        };
        // info!("outstanding: {outstanding_batches:?}, sbi: {selected_batch_index}, batches: {:?}", self.batches);
        // let selected_batch = outstanding_batches[selected_batch_index];
        let assignment = self.batches[&selected_batch_index]
            .iter()
            .map(|i| {
                let input = self.inputs.get(i).cloned().unwrap();
                input
            })        
            .collect();
        let batch_id = self.inverse_batch_ids.get(&selected_batch_index).unwrap();
        Some((*batch_id, assignment))
    }

    // batch has been successfully sent to prover
    pub fn confirm_assignment(&mut self, prover: &Vec<u8>, batch_id: u128) {
        let batch_index = self.batch_ids.get(&batch_id).unwrap();
        self.assigned_batches.entry(*batch_index)
            .and_modify(|batches| {
                batches.push(prover.clone());
            })
            .or_insert_with(|| {
                vec![prover.clone()]
            });
        self.prover_batch_assignements.entry(prover.clone())
            .and_modify(|batches|{
                batches.push(*batch_index);
            })
            .or_insert_with(|| {
                vec![*batch_index]
            });
    }

    pub fn is_finished(&self) -> bool {
        !self.batches.is_empty() &&
        self.batches.len() == self.proofs.len()
    }
}
