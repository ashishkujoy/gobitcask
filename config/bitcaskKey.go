package config

type Serializable interface {
	Serialize() []byte
}

type BitcaskKey interface {
	comparable
	Serializable
}
