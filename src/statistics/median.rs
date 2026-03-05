/// Online quantile estimator using a stochastic-gradient (SGD) update rule.
///
/// (from the paper "A perceptron-like online algorithm for tracking the median")
pub struct EstimatedMedianTracker {
    /// Current quantile estimate.
    q: f64,
    /// Target quantile in [0, 1].
    p: f64,
    /// Learning rate (step size in the same units as the samples).
    eta: f64,
}

impl EstimatedMedianTracker {
    pub fn new(initial: f64, p: f64, eta: f64) -> Self {
        assert!((0.0..=1.0).contains(&p), "p must be in [0,1]");
        assert!(eta > 0.0, "eta must be positive");
        Self { q: initial, p, eta }
    }

    pub fn update(&mut self, sample: f64) {
        let sign = if sample > self.q {
            1.0
        } else if sample < self.q {
            -1.0
        } else {
            0.0
        };
        self.q += self.eta * (sign + 2.0 * self.p - 1.0);
    }

    pub fn estimate(&self) -> f64 {
        self.q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converges_toward_true_quantile() {
        // Feed 10 000 samples of the same value (512 KiB).
        // The estimator should settle within 5% of that value.
        let target = (512 * 1024) as f64;

        let mut q = EstimatedMedianTracker::new(0.0, 0.5, 1024.0);

        for _ in 0..10_000 {
            q.update(target);
        }

        let est = q.estimate();
        let error_ratio = (est - target).abs() / target;
        assert!(
            error_ratio < 0.05,
            "median estimate {est} is more than 5% away from {target}"
        );
    }
}
