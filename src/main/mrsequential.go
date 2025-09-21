package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "../mr"        // 引用了上层目录下的 mr 包，通常含有 KeyValue 定义等
import "plugin"       // 动态加载 .so 插件
import "os"
import "log"
import "io/ioutil"    // 读取整个文件内容；Go1.16+ 推荐使用 io.ReadAll
import "sort"

// for sorting by key.
// KeyValue 列表按 Key 排序所需的类型和方法
// example [{key:a, value:1}, {key:b, value:1}]	
type ByKey []mr.KeyValue

// for sorting by key.
// 实现 sort.Interface
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	// 参数校验：至少需要 2 个参数：第一个为 .so 插件，后面是一个或多个输入文件
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// 动态加载插件，获取 Map 和 Reduce 函数
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	// 读取每个输入文件，调用 Map，将所有中间结果收集到一个切片中。
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		// 读取整个文件内容到内存（适用于示例/小文件）
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		// 调用用户实现的 Map 函数，返回一组 KeyValue
		kva := mapf(filename, string(content))
		// 合并到总体的 intermediate 列表
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	//
	// 与真实 MapReduce 的一个重要区别：这里所有中间数据都在一个切片中，
	// 而不是分成 N x M 个桶进行分区。
	//

	// 按 Key 排序，方便后续把相同 key 的 value 聚集在一起供 Reduce 使用
	sort.Sort(ByKey(intermediate))

	// 输出文件名是固定的 mr-out-0（MIT 6.824 实验要求的输出格式）
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	// 对 intermediate 中每个不同的 key 调用 Reduce，然后把结果写入 mr-out-0
	//
	i := 0
	for i < len(intermediate) {
		// 找到从 i 开始的连续相同 key 的区间 [i, j)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// 收集该 key 对应的所有 values
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 调用用户实现的 Reduce 函数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		// 按每行 "key value\n" 的格式写入输出文件（与课程/测试要求一致）
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		// 继续下一个不同的 key
		i = j
	}

	ofile.Close()
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	// 打开so文件
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	// 在插件中查找符号 Map
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	// 进行类型断言：期望 Map 的类型是 func(string, string) []mr.KeyValue
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	// 查找 Reduce 符号
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	// 类型断言：期望 Reduce 的类型是 func(string, []string) string
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
