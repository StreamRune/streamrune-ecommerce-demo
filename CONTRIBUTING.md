# Contributing to the StreamRune e-commerce demo

Thanks for helping to improve the demo and its tutorial! Bug reports, questions and pull requests
are welcome.

## Licensing of contributions

The demo is licensed under the Apache License 2.0 (see `LICENSE`). By contributing, you agree that
your contribution is licensed under the same license.

## Developer Certificate of Origin (DCO)

Every commit must be signed off ([DCO](https://developercertificate.org/)):

    git commit --signoff

A pull request check rejects commits without a `Signed-off-by` trailer.

## Commit messages

We use [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <summary>`,
for example `fix(spring-app): return 404 for an unknown order`. Run
`git config commit.template .gitmessage` once to get the template in your editor.

## Building

The demo builds against the framework in a sibling directory when it exists:

    git clone https://github.com/StreamRune/streamrune.git ../streamrune
    ./gradlew build

The tutorial in `docs/tutorial/` follows the code: when you change code a chapter shows, update
the chapter in the same pull request.
