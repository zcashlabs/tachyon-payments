//! Epoch arithmetic (spec section 3) and the confirmation rule (spec 11.2).
//!
//! `pool_height = height - activation_height` and
//! `epoch = pool_height / epoch_length`. The activation block has pool height
//! zero and begins epoch zero; heights below activation are in no epoch. The
//! epoch length is not final in the draft ZIPs, so it is a parameter, read from
//! configuration per network (spec section 15); [`EPOCH_LENGTH`] is only the
//! default.
use std::num::NonZeroU32;

/// The default epoch length, `zcash_tachyon::constants::EPOCH_SIZE`.
pub const EPOCH_LENGTH: u32 = 4096;

/// Blocks kept between the node's tip and the newest block ingested.
pub const CONFIRMATION_DEPTH: u32 = 2;

/// The newest height ingestion may read when the node's tip is `tip`:
/// `tip - CONFIRMATION_DEPTH`, saturating at zero.
pub const fn target_height(tip: u32) -> u32 {
    tip.saturating_sub(CONFIRMATION_DEPTH)
}

/// An epoch schedule that cannot exist.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EpochError {
    #[error("epoch length must be at least one block")]
    ZeroLength,
}

/// A network's epoch schedule: its Tachyon activation height and epoch length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EpochSchedule {
    activation_height: u32,
    epoch_length: NonZeroU32,
}

impl EpochSchedule {
    /// The schedule of a network that activates Tachyon at `activation_height`,
    /// with epochs of `epoch_length` blocks.
    pub fn new(activation_height: u32, epoch_length: u32) -> Result<Self, EpochError> {
        Ok(Self {
            activation_height,
            epoch_length: NonZeroU32::new(epoch_length).ok_or(EpochError::ZeroLength)?,
        })
    }

    /// The height of the first block with pool height zero.
    pub const fn activation_height(&self) -> u32 {
        self.activation_height
    }

    /// Blocks per epoch.
    pub const fn epoch_length(&self) -> u32 {
        self.epoch_length.get()
    }

    /// `height - activation_height`, or `None` below activation.
    pub const fn pool_height(&self, height: u32) -> Option<u32> {
        height.checked_sub(self.activation_height)
    }

    /// The epoch containing `height`, or `None` below activation.
    pub const fn epoch_of(&self, height: u32) -> Option<u32> {
        match self.pool_height(height) {
            Some(pool_height) => Some(pool_height / self.epoch_length.get()),
            None => None,
        }
    }

    /// The first height of `epoch`, or `None` if it is beyond `u32` heights.
    pub const fn first_height(&self, epoch: u32) -> Option<u32> {
        match epoch.checked_mul(self.epoch_length.get()) {
            Some(pool_height) => pool_height.checked_add(self.activation_height),
            None => None,
        }
    }

    /// The last height of `epoch`, or `None` if it is beyond `u32` heights.
    pub const fn last_height(&self, epoch: u32) -> Option<u32> {
        match self.first_height(epoch) {
            Some(first) => first.checked_add(self.epoch_length.get() - 1),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The testnet schedule: activation at 8, 4,096-block epochs.
    fn testnet() -> EpochSchedule {
        EpochSchedule::new(8, EPOCH_LENGTH).unwrap()
    }

    #[test]
    fn heights_below_activation_are_in_no_epoch() {
        let schedule = testnet();
        for height in 0..8 {
            assert_eq!(schedule.pool_height(height), None);
            assert_eq!(schedule.epoch_of(height), None);
        }
    }

    #[test]
    fn the_activation_block_begins_epoch_zero() {
        let schedule = testnet();
        assert_eq!(schedule.pool_height(8), Some(0));
        assert_eq!(schedule.epoch_of(8), Some(0));
        assert_eq!(schedule.first_height(0), Some(8));
        assert_eq!(schedule.last_height(0), Some(4103));
        assert_eq!(schedule.epoch_of(4103), Some(0));
        assert_eq!(schedule.epoch_of(4104), Some(1));
        assert_eq!(schedule.first_height(1), Some(4104));
        assert_eq!(schedule.last_height(1), Some(8199));
        assert_eq!(schedule.pool_height(39_586), Some(39_578));
        assert_eq!(schedule.epoch_of(39_586), Some(9));
    }

    /// Every height in an epoch maps back to it, and epochs tile the heights.
    #[test]
    fn epochs_tile_heights() {
        for (activation, length) in [(0, 1), (8, 4096), (8, 7), (1_000, 100)] {
            let schedule = EpochSchedule::new(activation, length).unwrap();
            for epoch in 0..20 {
                let first = schedule.first_height(epoch).unwrap();
                let last = schedule.last_height(epoch).unwrap();
                assert_eq!(last - first + 1, length);
                assert_eq!(schedule.epoch_of(first), Some(epoch));
                assert_eq!(schedule.epoch_of(last), Some(epoch));
                assert_eq!(schedule.first_height(epoch + 1), Some(last + 1));
            }
        }
    }

    #[test]
    fn epoch_length_is_a_parameter() {
        let schedule = EpochSchedule::new(8, 1024).unwrap();
        assert_eq!(schedule.epoch_length(), 1024);
        assert_eq!(schedule.activation_height(), 8);
        assert_eq!(schedule.epoch_of(1031), Some(0));
        assert_eq!(schedule.epoch_of(1032), Some(1));
        assert_eq!(EpochSchedule::new(8, 0), Err(EpochError::ZeroLength));
    }

    #[test]
    fn heights_beyond_u32_are_refused() {
        let schedule = testnet();
        assert_eq!(schedule.epoch_of(u32::MAX), Some((u32::MAX - 8) / 4096));
        let last_epoch = (u32::MAX - 8) / 4096;
        assert!(schedule.first_height(last_epoch).is_some());
        assert_eq!(schedule.first_height(last_epoch + 1), None);
        assert_eq!(schedule.first_height(u32::MAX), None);
        assert_eq!(schedule.last_height(u32::MAX), None);
        // The last epoch is partial: its last height does not fit.
        assert_eq!(schedule.last_height(last_epoch), None);
    }

    #[test]
    fn target_height_keeps_the_confirmation_depth() {
        assert_eq!(CONFIRMATION_DEPTH, 2);
        assert_eq!(target_height(39_586), 39_584);
        assert_eq!(target_height(2), 0);
        assert_eq!(target_height(1), 0);
        assert_eq!(target_height(0), 0);
    }
}
