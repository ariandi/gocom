package distobj

type TestDist interface {
	TestString(a, b string) string
	TestInt(a, b int) int
	TestBool(a, b bool) bool
	TestError(a, b int) (int, error)
	TestArray(a, b int) []int
	TestMap(a, b string) map[string]string
}
