# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

### Setting up pre-commit hooks

This repository uses [pre-commit](https://pre-commit.com/) to run security checks (e.g., secrets scanning via [git-secrets](https://github.com/awslabs/git-secrets)) before each commit. To set it up:

```bash
# Install git-secrets
brew install git-secrets  # macOS
# or: git clone https://github.com/awslabs/git-secrets && cd git-secrets && make install

# Install pre-commit and activate hooks
pip install pre-commit
pre-commit install

# Register AWS secret patterns
git secrets --register-aws
```

The pre-commit framework will automatically run git-secrets on staged files before each commit, preventing accidental secret leaks.

### If the secret scan flags a commit

The Secret Scan workflow runs gitleaks on every pull request and push to `main` and the `v*.*.x` release branches, and over the full history every Monday. Findings are attached to the workflow run as a SARIF report and appear in the repository's Security tab.

The pull request check fails when the files on the branch still contain the secret. When a secret was added and then removed within the same pull request, the check passes with a warning: the files are clean, but the secret is still in the branch's commit history.

Rotate the credential in both cases, before the pull request is merged. This repository is public, so a secret is exposed from the moment it is pushed, and deleting it from the files does not undo that.

Removing the secret from the history comes second: squash the commits that introduced it, or ask a maintainer to rewrite the branch. The Monday full-history run fails until it is gone.

Maintainers review secret-scan warnings and the Security tab, and confirm on the pull request that the credential was rotated before merging. If you are the contributor, say in the pull request thread that you have rotated it. Do not assume someone saw the warning.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Release process
`graphrag-lexical-graph` and `graphrag-byokg` are released separately, each with its own version and tags (`graphrag-lexical-graph/vX.Y.Z`, `graphrag-byokg/vX.Y.Z`). [lexical-graph/RELEASE.md](lexical-graph/RELEASE.md) and [byokg-rag/RELEASE.md](byokg-rag/RELEASE.md) have the steps for tagging and publishing. This section covers when releases happen and which branch a change belongs on.

### Release cadence

* An `X.Y.0` release goes out at least once a quarter, and can go out sooner. It passes a security review before it is published.
* `X.Y.Z` patch releases (for example `3.20.1`) carry fixes in between. They go out as needed and do not go through the security review.

### Branches

* `main` is the development branch.
* Each `X.Y` release line has a release branch named `vX.Y.x` (for example `v3.20.x`), cut from `main`. It holds the changes for `X.Y.0` and for the patch releases that follow it.
* Open your pull request against the release branch if the change is for that release line, and against `main` otherwise. The package test workflows run on pull requests to both.
* A release branch is merged back into `main` with a pull request so that `main` has everything that shipped. Merge that pull request with a merge commit. A squash leaves the release branch's commits out of `main`'s history, so the next merge from the same branch lists them again and can conflict.

### Tagging work to a release

The target release of an issue or pull request is its GitHub milestone. Milestones are named for the package and version, for example `lexical-graph 3.19.0`.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
