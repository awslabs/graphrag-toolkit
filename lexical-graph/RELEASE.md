# Release Process

This document describes the release process for the graphrag-toolkit components. The goals are to:

- Provide a consistent, repeatable process for tagging and publishing releases.
- Distinguish between *pre-releases* and full *releases*.
- Maintain separate tags and workflows for the `graphrag-lexical-graph` and `graphrag-byokg` projects, allowing each to be released independently.
- Ensure each release is validated before it is published.

## Lexical Graph Release Process

1. Create a release candidate tag for the pre-release (e.g., `graphrag-lexical-graph/vX.Y.Z.devN`).
2. Create a pre-release in GitHub using the release candidate tag, marking it as a pre-release.
3. Test the pre-release artefacts via CI workflow and manual testing.
4. Create the final release tag (e.g., `graphrag-lexical-graph/vX.Y.Z`).
5. Update the GitHub release to remove the pre-release label, promoting it to a full release.
6. Test the final release from PyPI.

### Step 1: Create a Pre-release Tag

All tags are created from the `main` branch, which is the ongoing development branch. Pre-release tags use a `.devN` suffix:

```bash
git checkout main
git pull origin main
git tag graphrag-lexical-graph/vX.Y.Z.devN
git push origin graphrag-lexical-graph/vX.Y.Z.devN
```

For example, `graphrag-lexical-graph/v1.2.0.dev0` for the first dev pre-release for version `1.2.0`.

Note: `graphrag-lexical-graph` is used to create a lexical graph release.  

### Step 2: Create a GitHub Pre-release

Create a new release in GitHub using the pre-release tag. Mark it as a pre-release.

This triggers the [Lexical Graph Pre-Release](/.github/workflows/lexical-graph-prerelease.yml) workflow, which builds the package and attaches the following artefacts to the release:

- Python distribution files (wheel and sdist tarball)
- `lexical-graph-examples` notebook zip (versioned and latest)
- `lexical-graph-hybrid-dev-examples` notebook zip (versioned and latest) 

### Step 3: Test the Pre-release Artefacts

#### Validate the pre-release via CI workflow and manual testing.

To install the pre-release wheel from the GitHub release:

```bash
pip install https://github.com/awslabs/graphrag-toolkit/releases/download/graphrag-lexical-graph%2FvX.Y.Z.devN/graphrag_lexical_graph-X.Y.Z.devN-py3-none-any.whl
```

For example, to install `v1.2.0.dev1`:

```bash
pip install https://github.com/awslabs/graphrag-toolkit/releases/download/graphrag-lexical-graph%2Fv1.2.0.dev1/graphrag_lexical_graph-1.2.0.dev0-py3-none-any.whl
```

#### To run the unit tests locally:

```bash
cd lexical-graph
PYTHONPATH=src python -m pytest -v tests/
```

#### To run integration tests manually:

The wheel artefacts should be tested against a well known release test suite to look for regressions.  Release managers should run the test suite against the "Short" and "Versioning" test suites. To run: 

```bash
sh build-tests.sh --test-file lexical.short,lexical.versioning
```

#### Attach integration test results to the github-release

We want to include `metadata.json` and `test-results` folders as a zip to the release. 

```bash
aws s3 sync s3://your-bucket/test-results/ ./test-results/ --exclude "*" --include "metadata.json" --include "test-results/*"
zip -r test-results.zip ./test-results/
```

Attach `test-results.zip` to the github release. 

### Step 4: Create the Final Release Tag

Once the pre-release has been validated, create the full release tag (without the `.devN` suffix):

```bash
git checkout main
git pull origin main
git tag graphrag-lexical-graph/vX.Y.Z
git push origin graphrag-lexical-graph/vX.Y.Z
```

### Step 5: Promote to Full Release

Update the GitHub release to remove the pre-release label, promoting it to a full release.

### Step 6: Test the Final Release from PyPI

Install the published package from PyPI:

```bash
pip install graphrag-lexical-graph==X.Y.Z
```

Sanity checks should be completed against the PyPI release artefacts to verify the package installs and functions correctly.

Attach integration test results to the github-release (`metadata.json` and test results folders) as a zip to the release. 

```bash
aws s3 sync s3://your-bucket/test-results/ ./local-results/ --exclude "*" --include "metadata.json" --include "test-results/*"
zip -r test-results.zip ./local-results/
```