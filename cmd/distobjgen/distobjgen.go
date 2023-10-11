package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
)

var ifaceName string

func init() {

	flag.StringVar(&ifaceName, "src", "", "input interface name")
}

func getTypeStr(field ast.Expr) string {

	ident, ok := field.(*ast.Ident)

	if ok {
		return ident.Name
	}

	_, ok = field.(*ast.InterfaceType)

	if ok {
		return "interface{}"
	}

	arr, ok := field.(*ast.ArrayType)

	if ok {
		return "[]" + getTypeStr(arr.Elt)
	}

	mapType, ok := field.(*ast.MapType)

	if ok {
		return "map[" + getTypeStr(mapType.Key) + "]" + getTypeStr(mapType.Value)
	}

	return ""
}

func main() {

	flag.Parse()

	if ifaceName == "" {
		flag.PrintDefaults()
	}

	cwd, err := os.Getwd()

	if err != nil {
		fmt.Println("Unable to get working directory")
		os.Exit(-1)
	}

	fmt.Println("distobj gen :", ifaceName, cwd)
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, cwd, nil, parser.ParseComments)

	if err != nil {
		fmt.Println("Error when parsing package :", err)
		return
	}

	for _, pkg := range pkgs {

		for _, fl := range pkg.Files {

			for _, dcl := range fl.Decls {

				genDecl, ok := dcl.(*ast.GenDecl)

				if !ok {
					continue
				}

				if genDecl.Tok != token.TYPE {
					continue
				}

				for _, spec := range genDecl.Specs {

					astSpec, ok := spec.(*ast.TypeSpec)

					if !ok {
						continue
					}

					if astSpec.Name.Name != ifaceName {
						continue
					}

					// start generate proxy --------------

					proxyName := ifaceName + "Proxy"

					strOut := `package ` + pkg.Name + `

import "github.com/ariandi/gocom/distobj"

type ` + proxyName + ` struct {
	className string
	prefix string
}

var __` + proxyName + `Map map[string]*` + proxyName + ` = map[string]*` + proxyName + `{}

func Get` + proxyName + `(prefix ...string) *` + proxyName + ` {

	targetPrefix := ""
	if len(prefix) > 0 {
		targetPrefix = prefix[0]
	}

	ret, ok := __` + proxyName + `Map[targetPrefix]

	if !ok {
		ret = &` + proxyName + `{}
		ret.className = "` + ifaceName + `"
		ret.prefix = targetPrefix

		__` + proxyName + `Map[targetPrefix] = ret
	}

	return ret
}

					`

					ast.Print(fset, fl)

					astIFace, ok := astSpec.Type.(*ast.InterfaceType)

					if !ok {
						continue
					}

					for _, field := range astIFace.Methods.List {

						if len(field.Names) == 0 {
							continue
						}

						mtdName := field.Names[0].Name
						paramList := map[string]string{}
						resultList := []string{}
						errorResult := ""
						normalResult := ""
						resultConverter := ""

						astMtd, ok := field.Type.(*ast.FuncType)

						if !ok {
							continue
						}

						//params
						for _, param := range astMtd.Params.List {

							paramType := getTypeStr(param.Type)

							for _, name := range param.Names {

								paramList[name.Name] = paramType
							}
						}

						//result
						for i, result := range astMtd.Results.List {

							typeStr := getTypeStr(result.Type)

							switch typeStr {
							case "string":
								errorResult += ", \"\""
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToStr(ret[%d])", i, i)
							case "int":
								errorResult += ", 0"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToInt(ret[%d])", i, i)
							case "int16":
								errorResult += ", 0"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToInt16(ret[%d])", i, i)
							case "int32":
								errorResult += ", 0"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToInt32(ret[%d])", i, i)
							case "int64":
								errorResult += ", 0"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToInt64(ret[%d])", i, i)
							case "float32":
								errorResult += ", 0"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToFloat32(ret[%d])", i, i)
							case "float64":
								errorResult += ", 0"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToFloat64(ret[%d])", i, i)
							case "bool":
								errorResult += ", false"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToBool(ret[%d])", i, i)
							case "error":
								errorResult += ", err"
								resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToErr(ret[%d])", i, i)
							case "interface{}":
								errorResult += ", nil"
								resultConverter += fmt.Sprintf("\n\tret%d := ret[%d]", i, i)
							default:
								fmt.Println("====>>> gen default : ", typeStr)
								if strings.HasPrefix(typeStr, "[]") ||
									strings.HasPrefix(typeStr, "map[") ||
									strings.HasPrefix(typeStr, "*") {
									errorResult += ", nil"

									if strings.HasPrefix(typeStr, "[]") {

										resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToArr(ret[%d], \"%s\").(%s)", i, i, typeStr[2:], typeStr)
									} else if strings.HasPrefix(typeStr, "map[") {

										posLast := strings.Index(typeStr, "]")
										keyType := typeStr[4:posLast]
										valType := typeStr[posLast+1:]
										resultConverter += fmt.Sprintf("\n\tret%d := distobj.ToMap(ret[%d], \"%s\", \"%s\").(%s)", i, i, keyType, valType, typeStr)
									} else {

										resultConverter += fmt.Sprintf("\n\tret%d := &%s{}", i, typeStr[1:])
									}
								} else {
									errorResult += ", " + typeStr + "{}"
									resultConverter += fmt.Sprintf("\n\tret%d := %s{}", i, typeStr)
								}
							}

							normalResult += fmt.Sprintf(", ret%d", i)
							resultList = append(resultList, typeStr)
						}

						// start generate method ----------------------------

						str := "func (o *" + proxyName + ") " + mtdName + "("

						paramDecl := ""
						paramInvoke := ""

						for pName, pType := range paramList {

							paramDecl += ", " + pName + " " + pType
							paramInvoke += ", " + pName
						}

						if len(paramList) > 0 {
							paramDecl = paramDecl[2:]
						}

						str += paramDecl + ")"

						resultDecl := ""

						for _, result := range resultList {

							resultDecl += "," + result
						}

						if len(resultList) > 0 {
							if len(resultList) > 1 {
								resultDecl = "(" + resultDecl[1:] + ") "
							} else {
								resultDecl = resultDecl[1:] + " "
							}

							errorResult = errorResult[2:]
							normalResult = "return " + normalResult[2:]
						}

						str += " " + resultDecl + `{

	ret, err := distobj.Invoke(o.prefix, o.className, "` + mtdName + `"` + paramInvoke + `)

	if err != nil {
		return ` + errorResult + `
	}

	` + resultConverter + `

	` + normalResult + `
}`

						strOut += `
` + str + `
						`
					}

					fmt.Println("====================================>")
					fmt.Println(strOut)

					err = os.WriteFile(proxyName+".go", []byte(strOut), 0644)

					if err != nil {
						fmt.Println("error write file : ", err)
					}
				}
			}
		}
	}
}
