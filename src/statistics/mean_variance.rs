/// Online mean/variance tracker using Welford's one-pass algorithm.
pub struct MeanVarianceTracker {
    count: u64,
    mean: f64,
    m2: f64,
}

impl MeanVarianceTracker {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    pub fn update(&mut self, x: f64) {
        self.count += 1;
        let old_mean = self.mean;
        self.mean += (x - self.mean) / self.count as f64;
        self.m2 += (x - old_mean) * (x - self.mean);
    }

    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Population variance. Returns `None` when no samples have been added.
    pub fn variance(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.m2 / self.count as f64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_value() {
        let mut state = MeanVarianceTracker::new();
        state.update(200.0);

        assert_eq!(state.mean(), 200.0);
        // Population variance of a single value is 0
        assert_eq!(state.variance(), Some(0.0));
    }

    #[test]
    fn multiple_values() {
        let mut state = MeanVarianceTracker::new();
        // Values: 100, 200, 300  →  mean = 200, pop. variance = 6666.̄6
        state.update(100.0);
        state.update(200.0);
        state.update(300.0);

        let mean = state.mean();
        assert!(
            (mean - 200.0).abs() < 1.0,
            "mean {mean} should be close to 200"
        );

        let variance = state.variance().unwrap();
        let expected_variance =
            ((100.0f64 - 200.0).powi(2) + (200.0f64 - 200.0).powi(2) + (300.0f64 - 200.0).powi(2))
                / 3.0;
        assert!(
            (variance - expected_variance).abs() < 1.0,
            "variance {variance} should be close to {expected_variance}"
        );
    }

    #[test]
    fn no_samples_returns_none_variance() {
        let state = MeanVarianceTracker::new();
        assert_eq!(state.variance(), None);
        assert_eq!(state.mean(), 0.0);
    }
}
