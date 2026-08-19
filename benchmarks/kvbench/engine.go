package kvbench

import (
	"fmt"
	"runtime/debug"
)

// EngineInfo identifies an adapter and whether this binary can run it.
type EngineInfo struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	Available   bool   `json:"available"`
	Unavailable string `json:"unavailable_reason,omitempty"`
}

type engineOptions struct {
	Durability    Durability
	CacheBytes    int64
	MemtableBytes int64
}

type engineFactory interface {
	Info() EngineInfo
	Open(dir string, opts engineOptions) (store, error)
}

type store interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Flush() error
	Close() error
}

func factories() map[string]engineFactory {
	items := []engineFactory{
		fuseDBFactory{},
		pebbleFactory{},
		badgerFactory{},
		rocksDBFactory(),
	}
	result := make(map[string]engineFactory, len(items))
	for _, item := range items {
		result[item.Info().Name] = item
	}
	return result
}

// Engines returns all known adapters in stable display order.
func Engines() []EngineInfo {
	registered := factories()
	result := make([]EngineInfo, 0, len(registered))
	for _, name := range []string{"fusedb", "pebble", "badger", "rocksdb"} {
		result = append(result, registered[name].Info())
	}
	return result
}

func selectFactories(names []string) ([]engineFactory, []EngineInfo, error) {
	registered := factories()
	selected := make([]engineFactory, 0, len(names))
	skipped := make([]EngineInfo, 0)
	for _, name := range names {
		factory, ok := registered[name]
		if !ok {
			return nil, nil, fmt.Errorf("unknown engine %q", name)
		}
		info := factory.Info()
		if !info.Available {
			skipped = append(skipped, info)
			continue
		}
		selected = append(selected, factory)
	}
	if len(selected) == 0 {
		return nil, skipped, fmt.Errorf("none of the requested engines is available")
	}
	return selected, skipped, nil
}

func dependencyVersion(path string) string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Path == path {
		return normalizeVersion(info.Main.Version, info.Main.Replace)
	}
	for _, dependency := range info.Deps {
		if dependency.Path == path {
			return normalizeVersion(dependency.Version, dependency.Replace)
		}
	}
	return "unknown"
}

func normalizeVersion(version string, replacement *debug.Module) string {
	if replacement == nil {
		return version
	}
	if replacement.Version != "" && replacement.Version != "(devel)" {
		return replacement.Version
	}
	if replacement.Path != "" {
		return "local:" + replacement.Path
	}
	return version
}
