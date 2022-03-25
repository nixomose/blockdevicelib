/* Package blockdevicelib has a comment to make the linter happy. */
package blockdevicelib

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"syscall"

	"github.com/nixomose/nixomosegotools/tools"
	"github.com/nixomose/stree_v/stree_v_lib/stree_v_lib"
	"github.com/nixomose/stree_v/stree_v_lib/stree_v_node"
)

func (this *Lbd_lib) Dump_header(cat *Catalog, device_name string) tools.Ret {
	var ret, catentry = this.get_catalog_entry(cat, device_name)
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENOENT) {
			return tools.Error(this.log, "device: ", device_name, " not found")
		}
		return ret
	}

	var device = this.New_block_device_from_catalog_entry(catentry)
	var block_size uint32 = device.Stree_calculated_node_size

	var fstore *stree_v_lib.File_store_aligned
	ret, fstore = this.make_file_store_aligned(device, block_size)
	if ret != nil {
		return ret
	}

	ret = fstore.Open_datastore_readonly()
	if ret != nil {
		return ret
	}

	defer fstore.Shutdown()

	var data []byte
	ret, data = fstore.Read_raw_data(0)
	if ret != nil {
		return ret
	}
	var m_header stree_v_lib.File_store_header
	var header_data = data[0:int(m_header.Serialized_size())]
	data = header_data

	ret = m_header.Deserialize(this.log, &data)
	if ret != nil {
		return ret
	}

	var m map[string]string = make(map[string]string)
	fmt.Println(tools.Dump(data))

	magic := make([]byte, 8)
	binary.BigEndian.PutUint64(magic, uint64(m_header.M_magic))
	// not quite what I wanted. m["0001_magic"] = hexdump.Dump(magic)
	m["0001_key"] = tools.Dump([]byte(magic))

	m["0002_store_size_in_bytes"] = tools.Prettylargenumber_uint64(uint64(m_header.M_store_size_in_bytes)) +
		" 0x" + fmt.Sprintf("%016x", m_header.M_store_size_in_bytes)
	m["0003_nodes_per_block"] = tools.Prettylargenumber_uint64(uint64(m_header.M_nodes_per_block)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_nodes_per_block)
	m["0004_block_size"] = tools.Prettylargenumber_uint64(uint64(m_header.M_block_size)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_block_size)
	m["0005_block_count"] = tools.Prettylargenumber_uint64(uint64(m_header.M_block_count)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_block_count)
	m["0006_root_node"] = tools.Prettylargenumber_uint64(uint64(m_header.M_root_node)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_root_node)
	m["0007_free_position"] = tools.Prettylargenumber_uint64(uint64(m_header.M_free_position)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_free_position)
	m["0008_alignment"] = tools.Prettylargenumber_uint64(uint64(m_header.M_alignment)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_alignment)
	m["0009_dirty"] = tools.Prettylargenumber_uint64(uint64(m_header.M_dirty)) +
		" 0x" + fmt.Sprintf("%08x", m_header.M_dirty)

	bytesout, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return tools.Error(this.log, "unable to marshal root node information into json: ", err)
	}

	var json string = string(bytesout)
	fmt.Println(json)
	return nil
}

func (this *Lbd_lib) Dump_block_header(cat *Catalog, device_name string, block_num uint32) tools.Ret {

	var ret, catentry = this.get_catalog_entry(cat, device_name)
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENOENT) {
			return tools.Error(this.log, "device: ", device_name, " not found")
		}
		return ret
	}

	var device = this.New_block_device_from_catalog_entry(catentry)
	var block_size uint32 = device.Stree_calculated_node_size
	var fstore *stree_v_lib.File_store_aligned
	ret, fstore = this.make_file_store_aligned(device, block_size)
	if ret != nil {
		return ret
	}

	ret = fstore.Open_datastore_readonly()
	if ret != nil {
		return ret
	}
	defer fstore.Shutdown()

	var data []byte
	ret, data = fstore.Read_raw_data(block_num)
	if ret != nil {
		return ret
	}

	var key_length, value_length, additional_nodes_per_block, _, _ = this.get_init_size_values(device)

	/* this is how much data in a single unit stree is going to ask the backing store to store.
		 it's the size of the actual block of data we want to store plus the stree header,
	   this is also called the stree_block_size */
	// var stree_calculated_node_size uint32
	// ret, stree_calculated_node_size = stree_v_lib.Calculate_block_size(this.log, key_type, value_type,
	// 	key_length, value_length, additional_nodes_per_block)
	// if ret != nil {
	// 	return ret
	// }

	var offspring_per_node = additional_nodes_per_block

	// var stree *stree_v_lib.Stree_v = &stree_v_lib.Stree_v{log: this.log}

	var n stree_v_node.Stree_node = *stree_v_node.New_Stree_node(this.log, "default_key", []byte("default_value"),
		key_length, value_length, offspring_per_node)
	ret = n.Deserialize(*this.log, &data)
	if ret != nil {
		return ret
	}

	var m map[string]string = make(map[string]string)
	var headerdatalength = n.Serialized_size_without_value(key_length, value_length)
	var headerdata []byte = data[0:headerdatalength]
	fmt.Println(tools.Dump(headerdata)) // only header size? xxxz

	m["0001_parent_block_num"] = tools.Prettylargenumber_uint64(uint64(n.Get_parent())) +
		" 0x" + fmt.Sprintf("%08x", n.Get_parent())
	m["0002_left_child_block_num"] = tools.Prettylargenumber_uint64(uint64(n.Get_left_child())) +
		" 0x" + fmt.Sprintf("%08x", n.Get_left_child())
	m["0003_right_child_block_num"] = tools.Prettylargenumber_uint64(uint64(n.Get_right_child())) +
		" 0x" + fmt.Sprintf("%08x", n.Get_right_child())
	m["0004_key_length"] = tools.Prettylargenumber_uint64(uint64(n.Get_key_length())) +
		" 0x" + fmt.Sprintf("%08x", n.Get_key_length())
	m["0005_value_length"] = tools.Prettylargenumber_uint64(uint64(n.Get_value_length())) +
		" 0x" + fmt.Sprintf("%08x", n.Get_value_length())
	m["0006_offspring_nodes"] = tools.Prettylargenumber_uint64(uint64(offspring_per_node)) +
		" 0x" + fmt.Sprintf("%08x", offspring_per_node)
	var offspring string
	var lp uint32
	for lp = 0; lp < offspring_per_node; lp++ {
		if lp > 0 {
			offspring += " "
		}

		var offspring_pos *uint32
		ret, offspring_pos = n.Get_offspring_pos(lp)
		if ret != nil {
			return ret
		}
		offspring += tools.Uint32tostring(*offspring_pos)
	}
	m["0007_offspring"] = offspring
	m["0008_key"] = tools.Dump([]byte(n.Get_key()))

	bytesout, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return tools.Error(this.log, "unable to marshal root node information into json: ", err)
	}

	var json string = string(bytesout)
	fmt.Println(json)

	return nil
}

func (this *Lbd_lib) Dump_block(cat *Catalog, device_name string, block_num uint32) tools.Ret {
	return nil
}
