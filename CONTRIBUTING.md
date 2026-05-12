# Contributing to FuseDB

## Development Setup

### Prerequisites

- Go 1.23 or later
- golangci-lint
- goimports

### Running Tests

```bash
make test
make test-coverage
```

### Running Benchmarks

```bash
make bench
make bench-latency
make bench-ycsb
```

### Code Quality

```bash
make fmt
make lint
make ci
```

## Commit Guidelines

- Use conventional commits: `type(scope): description`
- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`
- Keep commits focused

## Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `make ci`
5. Submit a pull request

## Code Style

- Follow standard Go conventions
- Use `gofmt` and `goimports`
- Write tests for new functionality

## License

MIT License
