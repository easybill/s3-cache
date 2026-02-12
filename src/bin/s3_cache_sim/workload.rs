use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Zipf};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Pattern {
    Uniform,
    Zipf,
    Scan,
}

pub fn generate_workload(
    pattern: Pattern,
    num_objects: usize,
    num_requests: usize,
    zipf_exponent: f64,
    one_hit_wonder_ratio: f64,
    seed: u64,
) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut requests = Vec::with_capacity(num_requests);
    let mut ohw_counter = num_objects; // one-hit-wonder indices start beyond normal objects

    let zipf_dist = if matches!(pattern, Pattern::Zipf) {
        Some(Zipf::new(num_objects as f64, zipf_exponent).expect("invalid zipf parameters"))
    } else {
        None
    };

    let mut scan_cursor: usize = 0;

    for _ in 0..num_requests {
        // Decide if this request is a one-hit-wonder
        if one_hit_wonder_ratio > 0.0 && rng.random::<f64>() < one_hit_wonder_ratio {
            requests.push(ohw_counter);
            ohw_counter += 1;
            continue;
        }

        let idx = match pattern {
            Pattern::Uniform => rng.random_range(0..num_objects),
            Pattern::Zipf => {
                // Zipf returns values in [1, num_objects]
                let sample: f64 = zipf_dist.as_ref().unwrap().sample(&mut rng);
                (sample as usize).saturating_sub(1).min(num_objects - 1)
            }
            Pattern::Scan => {
                let i = scan_cursor;
                scan_cursor = (scan_cursor + 1) % num_objects;
                i
            }
        };

        requests.push(idx);
    }

    requests
}
