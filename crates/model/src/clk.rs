//! Plan 43 C7a: the per-node clocks of the `cto` and `locks` models: an
//! oscillator with a rate error, and the three readings the code takes of it
//! (`constellation_types::clock`).
//!
//! Real time is the models' tick `t`. A node's oscillator runs at `t` plus
//! `rate` ticks every `rate_period` ticks (a staircase: it gains or loses its
//! ticks all at once at multiples of the period, the models' resolution), and
//! the node's readings are
//!
//! ```text
//! osc    = t + rate * (t / period)                  (period 0: t)
//! WALL   = off + (wall_follows_rate ? osc : t)      (what a step moves)
//! WAIT   = off0 + floor(osc * period / (period + budget))   runs slow by the budget
//! e_h    = ceil(osc * period / (period - budget))                runs fast by it
//! HONOUR = off0 + e_h + jump
//! jump   = max(jump, WALL - off0 - e_h, 0)          after every tick and step
//! ```
//!
//! `off` is the node's wall offset (a step, `Jump`, replaces it); `off0` is
//! where it started (HONOUR and WAIT count from it, and no step moves them);
//! `jump` is the running maximum of how far the wall has run ahead of the
//! honour clock's elapsed time (the code's `J`): HONOUR follows a forward step
//! once and ignores a backward one. With `clock_split` off all three readings
//! are the wall reading (the design before plan 43 C7-H1: one clock per node,
//! `local = t + off`), which with no rate is exactly what the models were.
//!
//! The budget is the code's rho in ticks per period. For an oscillator
//! within it, WAIT never over-counts and HONOUR never under-counts, **up to
//! one tick of rounding**: the staircase gains or loses a tick all at once
//! where a real oscillator does it a little at a time. The models' margins
//! (a tick or more) are the slack that pays for it, as the code's pay for
//! transit and millisecond rounding.

/// One node's clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Clk {
    /// The wall offset (what `Jump` sets).
    pub off: i16,
    /// The offset the node started with.
    pub off0: i16,
    /// The oscillator's error, ticks per `rate_period`.
    pub rate: i16,
    /// The honour clock's running maximum of the wall's lead (the code's `J`).
    pub jump: i16,
}

/// The clock parameters of a model.
#[derive(Clone, Copy, Debug)]
pub struct ClockSpec {
    pub rate_period: i16,
    pub rate_budget: i16,
    pub clock_split: bool,
    pub wall_follows_rate: bool,
}

impl ClockSpec {
    /// The oscillator's elapsed ticks at real time `t`.
    pub fn osc(&self, t: u8, c: &Clk) -> i16 {
        let t = t as i16;
        if self.rate_period > 0 {
            t + c.rate * (t / self.rate_period)
        } else {
            t
        }
    }

    /// The honour clock's elapsed ticks: the oscillator run fast by the budget.
    pub fn honour_elapsed(&self, t: u8, c: &Clk) -> i16 {
        let osc = self.osc(t, c);
        if self.rate_period > 0 {
            let (p, d) = (self.rate_period, self.rate_period - self.rate_budget);
            (osc * p + d - 1) / d
        } else {
            osc
        }
    }

    /// The WALL reading.
    pub fn wall(&self, t: u8, c: &Clk) -> i16 {
        let base = if self.wall_follows_rate {
            self.osc(t, c)
        } else {
            t as i16
        };
        c.off + base
    }

    /// The HONOUR reading (the wall's, with no split).
    pub fn honour(&self, t: u8, c: &Clk) -> i16 {
        if !self.clock_split {
            return self.wall(t, c);
        }
        c.off0 + self.honour_elapsed(t, c) + c.jump
    }

    /// The WAIT reading (the wall's, with no split).
    pub fn wait(&self, t: u8, c: &Clk) -> i16 {
        if !self.clock_split {
            return self.wall(t, c);
        }
        let osc = self.osc(t, c);
        let elapsed = if self.rate_period > 0 {
            osc * self.rate_period / (self.rate_period + self.rate_budget)
        } else {
            osc
        };
        c.off0 + elapsed
    }

    /// After time passes or the wall steps: every honour clock follows a
    /// wall that has run ahead of it (never back).
    pub fn follow_wall(&self, t: u8, clks: &mut [Clk]) {
        if !self.clock_split {
            return;
        }
        for c in clks.iter_mut() {
            let gap = self.wall(t, c) - c.off0 - self.honour_elapsed(t, c);
            c.jump = c.jump.max(gap).max(0);
        }
    }
}

/// The initial rate vectors: `init_rates` (or all zero) alone, or every
/// combination in `[-max_rate, max_rate]` per node under `explore_rates`.
pub fn initial_rates(
    n: usize,
    max_rate: i16,
    explore_rates: bool,
    init_rates: &Option<Vec<i16>>,
) -> Vec<Vec<i16>> {
    if explore_rates && max_rate > 0 {
        let mut rates: Vec<Vec<i16>> = vec![vec![]];
        for _ in 0..n {
            let mut next = Vec::new();
            for r in &rates {
                for d in -max_rate..=max_rate {
                    let mut v = r.clone();
                    v.push(d);
                    next.push(v);
                }
            }
            rates = next;
        }
        rates
    } else {
        vec![init_rates.clone().unwrap_or(vec![0; n])]
    }
}

/// The initial clocks of one node set: offsets and rates zipped.
pub fn initial_clocks(off: &[i16], rate: &[i16]) -> Vec<Clk> {
    off.iter()
        .zip(rate)
        .map(|(&o, &r)| Clk {
            off: o,
            off0: o,
            rate: r,
            jump: 0,
        })
        .collect()
}
