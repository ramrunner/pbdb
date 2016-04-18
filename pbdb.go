package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
	pb "pbdb/proto"
	"reflect"
	"strings"
	"os"
	"unicode"
	"unicode/utf8"
)

/*
	DECSTMT stname:stmt
	declares a statement for the db under the variable name
	DECDAT datname:pbname
	declares a protobuf named datname of type pbname.
	WRITE pb:field stname:fieldname
	writes part field of the protobuf named pb to stmt named stname under fieldname
	READ stname:fieldname datname:fieldname
	reads the field named fieldname from statement declared as stname to pb identified by datname on the field fieldname

*/

const (
	OP_READ = iota
	OP_WRITE
	OP_DECSTMT
	OP_DECDAT
	NUMOPS
)

var (
	logr    *log.Logger
	opnames = [NUMOPS]string{
		OP_READ:    "READ",
		OP_WRITE:   "WRITE",
		OP_DECSTMT: "DECSTMT",
		OP_DECDAT:  "DECDAT",
	}
)

type operation struct {
	action   int
	src      string
	srcfield string
	dst      string
	dstfield string
}

type protocas struct {
	statements map[string]string
	data       map[string]reflect.Type
	ops        []*operation
	command    string
}

func newProtocas() *protocas {
	return &protocas{
		statements: make(map[string]string),
		data:       make(map[string]reflect.Type),
		ops:        nil,
		command:    "",
	}
}


func (p *protocas) Parse(cmd string) ([]*operation, error) {
	cmdstrs := strings.Split(cmd, ";")
	var ops []*operation
	//remove whitespace around cmds
	cmdstrs = trimspaceslice(cmdstrs)
	for _, cmdstr := range cmdstrs {
		op, err := p.parsecmd(cmdstr)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func trimspaceslice(a []string) []string {
	for i := range a {
		a[i]=strings.TrimSpace(a[i])
	}
	return a
}


func (p *protocas) parsecmd(cmd string) (*operation, error) {
	parts := strings.Split(cmd, " ")
	//remove whitespace around args
	parts = trimspaceslice(parts)
	if len(parts) < 2 {
		return nil, fmt.Errorf("malformed command:%s with less than two parts", parts[0])
	}

	switch parts[0] {
	case "READ":
		if len(parts) != 3 {
			return nil, fmt.Errorf("malformed command. syntax: READ src:field dst:field")
		}
		return p.parseread(parts[1], parts[2])
	case "WRITE":
		if len(parts) != 3 {
			return nil, fmt.Errorf("malformed command. syntax: WRITE src:field dst:field")
		}
		return p.parsewrite(parts[1], parts[2])
	default:
		return nil, fmt.Errorf("unknown command:%s", parts[0])
	}
}
/*
func (p *protocas) parsedecstmt(stmt string) error {
	log.Printf("parsing cmd DECSTMT with args:%v",stmt)
	parts := strings.Split(stmt, ":")
	if len(parts)!=2 {
		return fmt.Errorf("args to DECSTMT are name:statement")
	}
	log.Printf("adding statement %s = %s", parts[0], parts[1])
	p.statements[parts[0]] = parts[1]
	log.Printf("declared statements: %+v", p.statements)
	return nil
}

func (p *protocas) parsedecdat(dat string) error {
	log.Printf("parsing cmd DECDAT with args:%v",dat)
	parts := strings.Split(dat, ":")
	if len(parts)!=2 {
		return fmt.Errorf("args to DECDAT are name:protoname")
	}
	log.Printf("adding data %s = %s", parts[0], parts[1])
	p.data[parts[0]] = parts[1]
	log.Printf("declared statements: %+v", p.statements)
	return nil
}
*/

func splitcolon(a string) ([]string, error) {
	parts := strings.Split(a, ":")
	if len(parts)!=2 {
		return nil, fmt.Errorf("args to operations are name:field")
	}
	return parts, nil
}

func (p *protocas) parsecolons(op *operation, src, dst string) (*operation, error) {
	var (
		 srcp, dstp []string
		 err error
	)
	if srcp, err = splitcolon(src); err != nil {
		return nil, err
	}
	if dstp, err = splitcolon(dst); err != nil {
		return nil, err
	}
	op.src, op.srcfield, op.dst, op.dstfield = srcp[0], srcp[1], dstp[0], dstp[1]

	//XXX: resolves the names here. maybe move to some other place.
	if err = p.resolveNames(op); err != nil {
		return nil, err
	}
	return op, nil
}

//resolves the symbol names in the protocas maps and errors if not found
func (p *protocas) resolveNames(op *operation) error {
	switch op.action {
	case OP_READ:
		if _, ok := p.statements[op.src]; !ok {
			return fmt.Errorf("READ operation got unknown source statement:%s", op.src)
		}
		//parse field
		if _, ok := p.data[op.dst]; !ok {
			return fmt.Errorf("READ operation got unknown destination data:%s", op.dst)
		}
		//parse field
	case OP_WRITE:
		if _, ok := p.data[op.src]; !ok {
			return fmt.Errorf("WRITE operation got unknown source data:%s", op.src)
		}
		//parse field
		if _, ok := p.statements[op.dst]; !ok {
			return fmt.Errorf("WRITE operation got unknown destination statement:%s", op.dst)
		}
		//parse field
	}
	return nil
}


func (p *protocas) parseread(src,dst string) (*operation,error) {
	log.Printf("parsing cmd READ with args: %v, %v",src, dst)
	op := &operation{action:OP_READ}
	return p.parsecolons(op, src, dst)
}

func (p *protocas) parsewrite(src,dst string) (*operation,error) {
	log.Printf("parsing cmd WRITE with args: %v, %v",src, dst)
	op := &operation{action:OP_WRITE}
	return p.parsecolons(op, src, dst)
}

func (p *protocas) AddDataType(name string, obj interface{}) error {
	if typ, ok := p.data[name]; ok {
		log.Printf("data type %s already contains type:%+v", name, typ)
		return fmt.Errorf("data name:%s already declared", name)
	}
	p.data[name] = reflect.TypeOf(obj)
	log.Printf("added data type:%+v named:%s", p.data[name], name)
	log.Printf("current declared data:%+v", p.data)
	return nil
}

func (p *protocas) AddStatement(name, value string) error {
	if val, ok := p.statements[name]; ok {
		log.Printf("statement named %s already contains value:%s", name, val)
		return fmt.Errorf("statement named:%s already declared", name)
	}
	p.statements[name] = value
	log.Printf("added statement:%s named:%s", value, name)
	log.Printf("current declared statements:%+v", p.statements)
	return nil
}

func (p *protocas) PrintOps() {
	log.Printf("debug printing operations")
	for _, o := range p.ops {
		log.Printf("\tOperation:%+v", o)
	}
}

func capitalizeFirst(a string) string {
	if len(a) < 1 {
		return ""
	}
	run, _ := utf8.DecodeRuneInString(a)
	return string(string(unicode.ToUpper(run)) + a[1:])
}

func init() {
	logr = log.New(os.Stdout, "pbdb:", log.LstdFlags)
}

func testRead(vals ...interface{}) {
	log.Printf("%v", reflect.ValueOf(vals[0]).Elem().CanSet())
	reflect.ValueOf(vals[0]).Elem().SetInt(12)
	log.Printf("vals is:%+v", vals)
}

func main() {
	//var dynamicMethod reflect.Value
	df := &pb.DataFoo{Label: proto.String("kotar")}
	//df1 := &pb.DataFoo{Label: proto.String("super kotar"), Type: proto.Int32(19)}
	//wrap := &protocas{data: map[string]interface{}{"foo":df}, statements:map[string]string{"foostmt":"i am cornholio"}}
	//wrap1 := &protocas{protostruct: df1, statement: "i am the king"}
	/*
		typewrap := reflect.TypeOf(*wrap)
		st, found := typewrap.FieldByName("statements")
		if !found {
			panic("protocas should have a field named statement")
		}
		funcname := st.Tag.Get("myfoo")
		getterfuncname := "Get" + capitalizeFirst(funcname)
		fmt.Printf("i will call %s\n", getterfuncname)
		props := proto.GetProperties(reflect.TypeOf(*df))
		fmt.Printf("i loaded pb:%+v. props:%+v\n", df, props)
		val := reflect.ValueOf(df)
		func1 := val.MethodByName(getterfuncname)
		fmt.Printf("calling the method:%v on:%+v\n", func1, df)
		fmt.Printf("result:%v", func1.Call([]reflect.Value{})[0])
	*/
	fmt.Printf("%v\n", df)
	pc := newProtocas()
	pc.AddDataType("kotor", pb.DataFoo{})
	pc.AddDataType("kastor", pb.DataBar{})
	pc.AddStatement("kotstmt", "foozong")
	ops, err := pc.Parse("WRITE kotor:field kotstmt:field; WRITE kastor:field kotstmt:field")
	if err != nil {
		logr.Fatal(err)
	}
	pc.ops = ops
	pc.PrintOps()
}
