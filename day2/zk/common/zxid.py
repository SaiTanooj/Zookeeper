"""zxid = ZooKeeper transaction id.

A zxid is a 64-bit number made of two halves:

    | epoch (high 32 bits) | counter (low 32 bits) |

* epoch   - goes up by one every time a new leader takes over.
* counter - goes up by one for every write that leader makes.

Comparing two zxids as plain integers therefore orders them first by epoch,
then by counter, which is exactly the global write order ZooKeeper promises.
"""

COUNTER_BITS = 32
COUNTER_MASK = (1 << COUNTER_BITS) - 1


def make_zxid(epoch, counter):
    return (epoch << COUNTER_BITS) | counter


def epoch_of(zxid):
    return zxid >> COUNTER_BITS


def counter_of(zxid):
    return zxid & COUNTER_MASK


def zxid_str(zxid):
    """Human-friendly form, e.g. 0x1:00000005 = epoch 1, write #5."""
    return f"0x{epoch_of(zxid):x}:{counter_of(zxid):08x}"
