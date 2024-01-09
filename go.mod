module go-dbms

go 1.19

replace (
	github.com/vahagz/bptree v0.0.4 => ./pkg/bptree
	github.com/vahagz/disk-allocator v0.0.2 => ./pkg/bptree/pkg/disk-allocator
	github.com/vahagz/pager v0.0.1 => ./pkg/bptree/pkg/disk-allocator/pkg/pager
)

require (
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.4
	github.com/vahagz/bptree v0.0.4
	github.com/vahagz/disk-allocator v0.0.2
	github.com/vahagz/pager v0.0.1
	github.com/x-cray/logrus-prefixed-formatter v0.5.2
	golang.org/x/exp v0.0.0-20230905200255-921286631fa9
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/edsrzf/mmap-go v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mgutz/ansi v0.0.0-20200706080929-d51e80ef957d // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.30.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/vahagz/rbtree v0.0.1 // indirect
	golang.org/x/crypto v0.16.0 // indirect
	golang.org/x/net v0.19.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/term v0.15.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
