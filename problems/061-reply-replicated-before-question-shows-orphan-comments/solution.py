"""Never show a reply in a region before the comment it replies to.

A Q&A site runs in three regions and replicates every new comment to the
other regions asynchronously, over independent links. A question posted in
Europe reached the US, someone in the US answered it, and the answer reached
Asia before the question did (the EU->APAC link was slower). Readers in Asia
saw answers "replying to a deleted comment", and the notification service
crashed looking up the missing parent.

The fix is causal delivery. Every write carries a version vector: how many
writes from each region its author's replica had applied when it was made.
A replica applies an incoming write only once it has applied everything that
write depended on; until then the write waits in a pending buffer.
"""
from __future__ import annotations

from dataclasses import dataclass, field

VersionVector = dict[str, int]


@dataclass(frozen=True)
class Write:
    """One new comment, plus the causal context it was written in."""
    origin: str
    vector: VersionVector
    comment_id: str
    parent_id: str | None
    text: str


@dataclass
class Replica:
    """One region's copy of the comments."""
    name: str
    regions: list[str]
    applied: VersionVector = field(default_factory=dict)
    comments: dict[str, Write] = field(default_factory=dict)
    pending: list[Write] = field(default_factory=list)
    apply_order: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        for region in self.regions:
            self.applied.setdefault(region, 0)

    def local_write(self, comment_id: str, text: str, parent_id: str | None = None) -> Write:
        """A user in this region posts; the write records what this region has seen."""
        self.applied[self.name] += 1
        write = Write(self.name, dict(self.applied), comment_id, parent_id, text)
        self._store(write)
        return write

    def _store(self, write: Write) -> None:
        self.comments[write.comment_id] = write
        self.apply_order.append(write.comment_id)

    def naive_receive(self, write: Write) -> None:
        """The original replication: show a write as soon as it arrives."""
        self._store(write)

    def already_applied(self, write: Write) -> bool:
        """A duplicate delivery of something we already have."""
        return self.applied[write.origin] >= write.vector[write.origin]

    def is_deliverable(self, write: Write) -> bool:
        """True when everything this write depends on has been applied here.

        It must be the next write from its origin (no gap in that region's
        sequence), and we must have applied at least as many writes from every
        other region as its author had seen.
        """
        if write.vector[write.origin] != self.applied[write.origin] + 1:
            return False
        for region in self.regions:
            if region != write.origin and self.applied[region] < write.vector[region]:
                return False
        return True

    def receive(self, write: Write) -> None:
        """Causal replication: buffer the write, then apply whatever is ready."""
        if self.already_applied(write):
            return
        self.pending.append(write)
        self._apply_ready_writes()

    def _apply_ready_writes(self) -> None:
        """Keep applying pending writes until none is ready.

        Applying one write can unblock others, so loop until a full pass makes
        no progress.
        """
        progress = True
        while progress:
            progress = False
            for write in list(self.pending):
                if self.is_deliverable(write):
                    self.pending.remove(write)
                    self.applied[write.origin] = write.vector[write.origin]
                    self._store(write)
                    progress = True


def orphan_replies(replica: Replica) -> list[str]:
    """Visible replies whose parent comment is not visible in this region."""
    orphans = []
    for comment_id, write in replica.comments.items():
        if write.parent_id is not None and write.parent_id not in replica.comments:
            orphans.append(comment_id)
    return orphans
