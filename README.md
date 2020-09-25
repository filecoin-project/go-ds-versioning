# go-ds-versioning
[![](https://img.shields.io/badge/made%20by-Protocol%20Labs-blue.svg?style=flat-square)](http://ipn.io)
[![CircleCI](https://circleci.com/gh/filecoin-project/go-ds-versioning.svg?style=svg)](https://circleci.com/gh/filecoin-project/go-ds-versioning)
[![codecov](https://codecov.io/gh/filecoin-project/go-ds-versioning/branch/master/graph/badge.svg)](https://codecov.io/gh/filecoin-project/go-ds-versioning)

Tools for building migratable datastore

## Description

This module provides building blocks for migrating key-value datastores that match the interfaces defined in [go-datastore](https://github.com/ipfs/go-datastore)

## Table of Contents
* [Background](#background)
* [Installation](#installation)
* [Usage](#usage)
    * [Defining migrations](#defining-migrations)
    * [Executing migrations on Datastores, StateStores, and Finite State Machine groups](#executing-migrations)
* [Architecture](#architecture)
* [Contribute](#contribute)

## Background

In the Filecoin ecosystem, we maintain a number of key-value datastores that
track the off-chain states of various components of Filecoin. As the protocol evolves, we will need to migrate these datastores. The Filecoin protocol requires nodes on the network maintain near constant uptime in order to meet proving requirements. We'd like to be able to migrate off-chain states while taking down only the components of the node that use these states.

This library provides two core components:

1. Tools for defining migrations. `go-datastore` is a freeform key-value database, so a migration could potentially mean any kind of transformation on
the database. However, most commonly, we want to migrate one record type to another. This library provides tools for doing that easily, and for composing these migrations together with versions to build a system for a versioned datastore that can be migrated easily with minimal potential for data-loss

2. Tools for migrating datastores, statestores, and finite state machines transparently, without changing the interfaces for those stores. We define versions of these stores that simply error until they are migrated. This means we can run migrations seperately and when they finish (if they finish successfully), the store comes online.

## Installation

**Requires go 1.14**

Install the module in your package or app with `go get "github.com/filecoin-project/go-ds-versioning"`

## Usage

For our example, let's imagine a key value store of records with the following structures:

```golang
type FruitType string

type FruitBasketOld struct {
    Type FruitType
    Count uint64
    Price big.Int
}
```

We want to support multiple fruits in the basked so our new structure is as follows:

```golang
type FruitBasket struct {
    Types []FruitType
    Count uint64
    Price big.Int
}
```

We've generated cbor-gen code for both of these. Now we have a function to migrate one to the other:

```golang
func MigrateFruitBasket(old * FruitBasketOld) (*FruitBasket, error) {
    return &FruitBasket{
        Types: []FruitType{old.Type},
        Count: old.Count,
        Price: old.Price,
    }, nil
}
```

### Defining Migrations

Now we want to migrate our store. Let's say we've never migrated our store before, so this is the first time we're going to migrate it.

Let's defined a initial set of migration using go-ds-versioning's builders:

```golang
import (
	versioning "github.com/filecoin-project/go-ds-versioning/pkg"
	"github.com/filecoin-project/go-ds-versioning/pkg/versioned"
)

builder := versioned.NewVersionedBuilder(MigrateFruitBasket, versioning.VersionKey("1"))

migrationBuilders := versioned.BuilderList{builder}

migrations, err := migrationBuilders.Build()
```

There's a lot happening there so let's take a look at this. We build a migration here:

```golang
builder := versioned.NewVersionedBuilder(MigrateFruitBasket, versioning.VersionKey("1")),
```

We're defining a migration that will use MigrateFruitBasket as its transformation function and will migration to version `1`. While not shown here, there are other things we can also do with this builder:

```golang
// if we want to provide a mechanism to reverse this migration
builder := builder.Reversable(UnMigrateFruitBasket)

// after our initial version, we always want to make sure we define the old version we're migrating from
version2Builder := version2Builder.OldVersion("1")

// we may need to for whatever reason, leave certain keys out of a migration
// (i.e. delete them, or we may need to exclude some keys that are at the same
// namespace hierarchy in our initial migration)
builder := builder.FilterKeys("rotten-fruit-basket")
```

We're assuming we'll probably define all our migrations in one place, so we make a `BuilderList` -- a list of migration definitions assembled using our builder interface. Typically you can just put your builders inline in the BuilderList.

Finally, to turn these into actual migrations, we call `.Build()` on the `BuilderList` -- this will ensure that our migrations are valid. In particular, every migration function must have the form:

```golang
func <T extends cbg.CBORUnmarshaller, U extends cbg.CBORMarshaller>(old T) (new U, error)
```

### Executing Migrations

Let's say we are using a [go-statestore](https://github.com/filecoin-project/go-statestore) for our fruit baskets:

```golang
import (
    "github.com/ipfs/go-datastore"
    "github.com/filecoin-project/go-statestore"
)

var ds datastore // this was setup elsewhere
fruitBaskets := statestore.New(ds)
```

To make it migratable, we replace it with a versioned statestore, with our migrations:

```golang
import (
    "github.com/ipfs/go-datastore"
    versioning "github.com/filecoin-project/go-ds-versioning/pkg"
    "github.com/filecoin-project/go-ds-versioning/pkg/statestore"
)

var ds datastore // this was setup elsewhere
var migrations versioning.VersionedMigrationList // the migrations we setup previously

fruitBaskets, migrateFruitBaskets := statestore.NewVersionedStateStore(ds, migrations, versioning.VersionKey("1"))
```

Here we're setting up a migrated statestore using the datastore, a list of migrations, and a target version. (target cause in certain cases we want to migrate to a particular version, either UP or DOWN).

We get back our statestore, and a function to run migrations so we're at our target version.

While the returned statestore has all the functions of our statestore, till we migrate, they won't work. After we migrate, we can use them:

```golang

has, err := fruitBaskets.Has("monday basket")
// has = false, err = versioning.ErrMigrationsNotRun

var ctx context.Context
migrationErr := migrateFruitBaskets(ctx)

if migrationErr != nil {
    has, err := fruitBaskets.Has("monday basket")
    // has = false, err = migrationErr
} else {
    has, err := fruitBaskets.Has("monday basket")
    // returns whatever the underlying state store would return
}
```

The assumption here is we'll want to setup our store in a constructor of a module, but in the context of Lotus, not want to run migrations until we get to some lifecycle hook. We can block in the lifecyle hook to insure migraitons are successful, but we may want to run them in a go-routine so Lotus can get up and running, and deal with the repercusions of failing migrations later.

`go-ds-versioning` also provides these abstractions for raw datastores, and for finite state machines defined with the DSL in `go-statemachine`

## Architecture

Under the hood, `go-ds-versioning` is creating new records within a versioned name space. Let's say our datastore has the following keys:

```
"/apples"
"/oranges"
```

And we're migrating with single initial migration to version "1". After migrating, the data base will look as follows:

```
"/versions/current" // value == "1"
"/1/apples"
"/1/oranges"
```

Not that the initial step of the migration is non-destructive -- we will copy rather than move when we transform. The old keys are only deleted after we know the ENTIRE migration is successful. If we have multiple migrations, we only delete keys after each step succeeds entirely.

Now if we migrate again later, we'll use "/versions/current" to figure out what we're migrating from. We might also use it if we wanted the ability to downgrade to an older version in order to run an older version of the code.

The basic rules are:
- assume anything could go wrong, including migration errors 
- assume we could get terminated in the middle (we include context as a parameter for this reason)
- try to avoid worst possible outcomes, in particular data loss.

## Contributing

PRs are welcome!  Please first read the design docs and look over the current code.  PRs against 
master require approval of at least two maintainers.  For the rest, please see our 
[CONTRIBUTING](https://github.com/filecoin-project/go-ds-versioning/CONTRIBUTING.md) guide.

## License
This repository is dual-licensed under Apache 2.0 and MIT terms.

Copyright 2019. Protocol Labs, Inc.
