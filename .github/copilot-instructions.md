# Copilot instructions for fab-shuffle

## Never assume what Fabric does. Check.

Fabric changes constantly, and this repository has already been broken twice by reasoning
from how the service used to behave.

Before writing code that depends on a service behaviour — "Fabric creates X automatically",
"this item type supports Y", "that API accepts Z" — **verify it against Microsoft Learn**,
using the `ms-learn` MCP server rather than general web search. If a behaviour cannot be
confirmed, say so rather than building on it.

This applies with particular force to anything of the form *"Fabric does this for us"*, which
is exactly the class of assumption that silently expires.

Worked examples from this repository, all of which cost a rebuild:

- Default semantic models were assumed to be auto-created alongside every lakehouse and
  warehouse. Fabric stopped creating them on 5 September 2025 and decoupled the existing ones
  into independent models by 30 November 2025. Code that skipped them was quietly refusing to
  migrate models people were using.
- The SQL database in Fabric connector was assumed to accept a service principal because the
  tenant reported that the `SQL` connection type does. That is the *Azure* SQL connector; the
  Fabric one takes an organizational account only, so the whole approach was unusable.
- `relations/downstream` was assumed to mean "what this writes to". It means "what depends on
  this", which for a lakehouse is every report that reads it. The check produced a page of
  confident nonsense.

### Trust per-endpoint pages over summary tables

The [item-management overview matrix](https://learn.microsoft.com/rest/api/fabric/articles/item-management/item-management-overview#item-types-api-support)
is stale on service principal support — it marks MirroredDatabase, Reflex and
MirroredAzureDatabricksCatalog as unsupported when all three endpoint pages say otherwise.
Learn's connector tables have been wrong too. When a summary and a specific page disagree,
the specific page wins, and the disagreement is worth a comment in the code.

## Report what the service said, do not predict it

When a call fails, repeat the service's own `errorCode` and message. Our reading of a status
code is a guess; the service's words are not. Where we do add an interpretation, it goes
*alongside* what the service said, never instead of it.

Correspondingly: do not refuse an operation on the grounds that it will probably fail. Attempt
it and explain the failure. The exception is when we can prove the request is malformed from
data we already hold — see below.

## The new workspace must be self-contained

A migrated item must either be fully repointed at the new workspace or not created at all.

It must never be left referencing the workspace being migrated away from: that looks like
success and breaks the day the source is deleted, which is the entire point of the move. If a
definition names something that did not migrate, refuse it and say what it needed.

## Warnings must be actionable

Every warning tells the operator something they can do. "This item type is in preview" is not
a warning. "Grant the service principal the User role on connection X, then re-run" is.

Prefer naming the item and the fix over describing the mechanism.

## Live things arrive stopped

Anything that acts on the world — mirrored databases, Reflex rules, Azure Databricks catalog
sync — is created switched off, and the operator is told how to start it. A copy that starts
replicating or firing rules the moment it exists is doing the original's job twice.

## Working practice

- Phase order in `orchestrator.py` is load-bearing and documented in its module docstring.
  A phase may only reference items created by an earlier one. `tests/test_rebuild_ordering.py`
  pins it.
- Every change needs tests, and a failure worth fixing is worth a test that reproduces it.
- Run `python -m pytest -q` and `python -m ruff check .` before rebuilding the container.
- Commit messages explain *why*, in prose, including what was rejected and what is still
  unverified.
