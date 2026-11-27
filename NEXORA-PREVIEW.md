# Nexora v0.4.0-preview compatibility candidate

Test fork of CN IM Hub 2026.11.0, based on the PR #28 asynchronous dispatcher branch. The upstream PR is unchanged. Install only as a complete replacement of the existing cn_im_hub directory on a backed-up test installation; keep its configuration entries, credentials and storage. The upstream manifest version is intentionally unchanged: identify this test build by its exact commit, not the displayed version.

Configured Agent must be Nexora with menu/context/semantic/guard protocol v1 to enable these optional lanes. Strict native queries retain priority after explicit menu/follow-up instructions. Local stateful turns are ordered per sender and do not coalesce by text; AI remains independent. Plain-text protocol only. Restart recovery reclassifies old queued entries and does not replay already-running entries. An upstream/other agent retains the existing behavior. AI fallback heuristic is not an authorization boundary; do not test arbitrary actions on live devices.

See the matching Nexora candidate docs/preview-acceptance.md. Real HA and WeChat validation is required; scheduler regression tests use mocked HA and WeChat.
