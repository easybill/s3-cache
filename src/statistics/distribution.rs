use psqr::P2 as QuantileEstimator;

/// Online quantile estimator (P² algorithm) with mean/variance tracking via
/// Welford's one-pass algorithm.
pub struct SizeDistributionTracker {
    // Welford mean/variance
    count: u64,
    mean: f64,
    m2: f64,

    // Quantile estimator
    quantile_estimator: QuantileEstimator,
}

impl SizeDistributionTracker {
    pub fn new(quantile: f64) -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
            quantile_estimator: QuantileEstimator::new(quantile),
        }
    }

    pub fn update(&mut self, sample: f64) {
        self.count += 1;

        let old_mean = self.mean;
        self.mean += (sample - self.mean) / (self.count as f64);
        self.m2 += (sample - old_mean) * (sample - self.mean);

        self.quantile_estimator.append(sample);
    }

    /// Returns the estimated quantile.
    pub fn estimate_quantile(&mut self) -> f64 {
        self.quantile_estimator.value()
    }

    /// Mean of all samples seen so far.
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Population variance of all samples seen so far.
    ///
    /// Returns `None` when no samples have been added yet.
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
    fn converges_toward_true_quantile() {
        let target = (512 * 1024) as f64;

        let mut q = SizeDistributionTracker::new(0.5);

        for _ in 0..10_000 {
            q.update(target);
        }

        let est = q.estimate_quantile();
        let error_ratio = (est - target).abs() / target;
        assert!(
            error_ratio < 0.05,
            "median estimate {est} is more than 5% away from {target}"
        );
    }

    #[test]
    fn mean_and_variance_are_correct() {
        let mut tracker = SizeDistributionTracker::new(0.5);

        tracker.update(200.0);
        assert_eq!(tracker.mean(), 200.0);
        assert_eq!(tracker.variance(), Some(0.0));

        let mut tracker = SizeDistributionTracker::new(0.5);
        tracker.update(100.0);
        tracker.update(200.0);
        tracker.update(300.0);

        let mean = tracker.mean();
        assert!(
            (mean - 200.0).abs() < 1.0,
            "mean {mean} should be close to 200"
        );

        let variance = tracker.variance().unwrap();
        let expected =
            ((100.0f64 - 200.0).powi(2) + (200.0f64 - 200.0).powi(2) + (300.0f64 - 200.0).powi(2))
                / 3.0;
        assert!(
            (variance - expected).abs() < 1.0,
            "variance {variance} should be close to {expected}"
        );
    }

    #[test]
    fn no_samples_initial_state() {
        let tracker = SizeDistributionTracker::new(0.5);
        assert_eq!(tracker.mean(), 0.0);
        assert_eq!(tracker.variance(), None);
    }
}
