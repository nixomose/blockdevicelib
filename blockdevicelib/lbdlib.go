package blockdevicelib

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"syscall"

	"github.com/nixomose/nixomosegotools/tools"
	stree_v_interfaces "github.com/nixomose/stree_v/stree_v_lib/stree_v_interfaces"
	"github.com/nixomose/stree_v/stree_v_lib/stree_v_lib"
	"github.com/nixomose/zosbd2goclient/zosbd2_stree_v_storage_mechanism"
	"github.com/nixomose/zosbd2goclient/zosbd2cmd/storage"
	"github.com/nixomose/zosbd2goclient/zosbd2cmdlib"
	"github.com/nixomose/zosbd2goclient/zosbd2cmdlib/zosbd2interfaces"

	"github.com/BurntSushi/toml"
	"github.com/spf13/cobra"
)

const PHYSICAL_BLOCK_SIZE = 4096
const ONE_MEG = (1024 * 1024)
const TIMEOUT_IN_SECONDS = 1200
const DEFAULT_NODE_VALUE_SIZE = 64 * 1024 // 64k

const TXT_ROOT = "root"
const TXT_ROOT_UID = "0"

const TXT_DEVICE_PATH_PREFIX = "/dev/"
const TXT_DEFAULT_CONTROL_DEVICE = TXT_DEVICE_PATH_PREFIX + "zosbd2ctl"

const TXT_MOUNT_CMD = "mount"
const TXT_UMOUNT_CMD = "umount"

/* block device commands */

const CMD_DEVICE_STATUS = "device-status"
const CMD_STORAGE_STATUS = "storage-status"
const CMD_CREATE_BLOCK_DEVICE = "create-device"
const CMD_DESTROY_BLOCK_DEVICE = "destroy-device"
const CMD_DESTROY_ALL_BLOCK_DEVICES = "destroy-all-devices"

const CMD_CATALOG_LIST = "catalog-list"

/* diagnostics and subcommands */

const CMD_DIAG = "diag"
const SUB_CMD_DUMP = "dump"
const SUB_CMD_DUMP_HEADER = "header"
const SUB_CMD_DUMP_BLOCK_HEADER = "blockheader"
const SUB_CMD_DUMP_BLOCK = "block"

/* catalog and subcommands */

const CMD_CATALOG = "catalog"
const SUB_CMD_CATALOG_LIST = "list"
const SUB_CMD_CATALOG_ADD = "add"
const SUB_CMD_CATALOG_DELETE = "delete"

/* block device catalog commands. */

const SUB_CMD_CATALOG_START = "start" // by name
const SUB_CMD_CATALOG_STOP = "stop"

/* configuration and catalog entry settings */

const CMD_SET = "set"
const CMD_CATALOG_ENTRY = "catalog-entry"
const CMD_EXCLUDE = "exclude"
const CMD_INCLUDE = "include"

// command line flags

const TXT_DEVICE_NAME = "device-name"
const TXT_DEVICE_SIZE = "device-size"
const TXT_STORAGE_FILE = "storage-file"
const TXT_DIRECTIO = "directio"
const TXT_SYNC = "sync"
const TXT_ALIGNMENT = "alignment"

const TXT_ALL = "all"

const TXT_NODE_VALUE_SIZE = "node-value-size" // provided by user in how much data they want in one mother node
const TXT_ADDITIONAL_NODES_PER_BLOCK = "additional-nodes-per-block"

const TXT_MOUNT = "mount"
const TXT_MOUNTPOINT = "mountpoint"

const TXT_FORCE = "force"

const TXT_DEVICE_RAMDISK = "device-ramdisk"
const TXT_STREE_RAMDISK = "stree-ramdisk"

const TXT_DRAGONS = "here-be-dragons"

const TXT_I = "I"
const TXT_AM = "Am"
const TXT_SURE = "Sure"

// const TXT_INCLUDE = "include"
// const TXT_EXCLUDE = "exclude"

type Lbd_lib struct {
	log *tools.Nixomosetools_logger

	Config_file string
	Log_file    string
	Log_level   uint32

	conf *Lbd_config

	default_config_file  string
	default_log_file     string
	default_catalog_file string

	application_name string

	control_device string   // "/dev/zosbd2ctl"
	catalog_file   string   // "/etc/localblockdevice/catalog.toml"
	catalog        *Catalog // the current in memory catalog.

	data_pipeline *list.List
}

type Lbd_device struct { // implements zosbd2interfaces.Device_interface

	// settings for the device
	Device_name        string
	Size               uint64
	Local_storage_file string
	Directio           bool
	Sync               bool
	Alignment          uint32 // if directio is false this should be the stree_block_size

	Stree_value_size           uint32 // this is the number of bytes of user data we store in a mother node
	Stree_calculated_node_size uint32 // this is the calculated size including the key and value size and other per-node header information
	Additional_nodes_per_block uint32

	Mount      bool   // should we try and mount after "catalog start", and similarly unmount on "catalog stop"
	Mountpoint string // where we should mount it

	Exclude_from_start_all bool // by default we start all devices for start --all unless this is set.

	// the objects that operate on this device, we need to keep the stree_v for shutdown
	stree   *stree_v_lib.Stree_v // we have to save this so we can shut it down cleanly on exit
	storage zosbd2interfaces.Storage_mechanism

	// for testing.
	device_ramdisk bool // replace the stree with a ramdisk
	stree_ramdisk  bool // replace the disk backing storage with a ramdisk
}

func (this *Lbd_device) Get_node_size_in_bytes() uint32 {
	var max_node_size_in_byte = this.Stree_value_size * (this.Additional_nodes_per_block + 1)
	return max_node_size_in_byte // this is the max amount of user data we store in a stree tree node
	// it will/can span the mother node and the offspring in its offspring list.
}

var _ zosbd2interfaces.Device_interface = &Lbd_device{}
var _ zosbd2interfaces.Device_interface = (*Lbd_device)(nil)

func New_blockdevicelib(application_name string) (tools.Ret, *Lbd_lib) {

	var retlib Lbd_lib
	retlib.application_name = application_name
	retlib.log = nil
	// retlib.opts = nil
	retlib.control_device = ""
	retlib.catalog = nil // this gets set after log init
	retlib.data_pipeline = nil

	return nil, &retlib
}

func (this *Lbd_lib) Get_log() *tools.Nixomosetools_logger {
	return this.log
}

func (this *Lbd_lib) Set_log(l *tools.Nixomosetools_logger) {
	this.log = l
}

func (this *Lbd_lib) Get_config_file() string {
	return this.Config_file
}

func (this *Lbd_lib) parse_config_file() {
	_, err := toml.DecodeFile(this.Config_file, &this.conf)
	if err != nil {
		this.log.Error("Unable to parse config file: ", this.Config_file, ", err: ", err)
		return
	}

	this.control_device = this.conf.Zosbd2.Control_device
	if len(this.control_device) == 0 {
		this.control_device = TXT_DEFAULT_CONTROL_DEVICE
	}

	this.catalog_file = this.conf.Catalog.Catalog_file
	if len(this.catalog_file) == 0 {
		this.catalog_file = this.default_catalog_file
	}
}

func (this *Lbd_lib) init_config_and_log() {
	if this.Config_file != "" {
		// Use config file from the flag.
	} else {
		this.Config_file = this.default_config_file
	}

	if this.Log_file != "" {
		// Use config file from the flag.
	} else {
		this.Log_file = this.default_log_file
	}

	this.log = tools.New_Nixomosetools_logger(int(this.Log_level))
	this.parse_config_file()

	this.catalog = New_catalog(this.log, this.catalog_file)

}

func (this *Lbd_lib) check_requirements() tools.Ret {
	var user, err = user.Current()
	if err != nil {
		return tools.Error(this.log, "unable to get user information: ", err)
	}
	if user.Uid != TXT_ROOT_UID {
		return tools.Error(this.log, "you must be root to run ", this.application_name)
	}
	return nil
}

func (this *Lbd_lib) Startup(default_config_file string, default_log_file string,
	default_catalog_file string) (tools.Ret, *cobra.Command) {
	var root_cmd = &cobra.Command{Use: this.application_name,
		Short: "lbd creates a block device backed by a file or a block device.",
		Long: "local block device allows you to define a catalog of block devices defining " +
			"their size and backing store and lets you easily start up and shut down these block " +
			"devices. requires zosbd2 - https://github.com/nixomose/zosbd2"}

	this.default_config_file = default_config_file
	this.default_log_file = default_log_file
	this.default_catalog_file = default_catalog_file

	root_cmd.PersistentFlags().StringVarP(&this.Config_file, "config-file", "c", this.default_config_file, "configuration file")
	root_cmd.PersistentFlags().StringVarP(&this.Log_file, "log-file", "l", this.default_log_file, "log file")
	root_cmd.PersistentFlags().Uint32VarP(&this.Log_level, "log-level", "v", 0, "log level: 0=debug 200=info 500=error")

	if ret := this.cobra_commands_setup(root_cmd); ret != nil {
		return ret, nil
	}

	var ret = this.check_requirements()
	if ret != nil {
		return ret, nil
	}
	return nil, root_cmd
}
func (this *Lbd_lib) Run(root_cmd *cobra.Command, data_pipeline *list.List) tools.Ret {

	this.data_pipeline = data_pipeline
	if this.data_pipeline == nil {
		return tools.Error(this.log, "no data pipline provided. can not process block requests without a data pipeline. ")
	}
	if err := root_cmd.Execute(); err != nil {
		return tools.Error(this.log, "error parsing command line: ", err)
	}
	return nil
}

func (this *Lbd_lib) New_block_device_from_catalog_entry(catentry *Catalog_entry) *Lbd_device {
	var device Lbd_device

	device.Device_name = catentry.Device_name
	device.Size = catentry.Size
	device.Local_storage_file = catentry.Local_storage_file
	device.Directio = catentry.Directio
	device.Sync = catentry.Sync

	device.Alignment = catentry.Alignment

	device.Stree_value_size = catentry.Node_value_size_bytes
	device.Stree_calculated_node_size = catentry.Node_calculated_size_bytes
	device.Additional_nodes_per_block = catentry.Additional_nodes_per_block

	device.stree = nil
	device.storage = nil

	device.Mount = catentry.Mount
	device.Mountpoint = catentry.Mountpoint

	/* for testing */
	device.device_ramdisk = false
	device.stree_ramdisk = false

	return &device
}

func (this *Lbd_lib) New_block_device(device_name string, size uint64,
	local_storage_file string, directio bool, sync bool, alignment uint32, stree_value_size uint32,
	calculated_stree_node_size uint32,
	additional_nodes_per_block uint32,
	mount bool, mountpoint string,
	device_ramdisk bool, stree_ramdisk bool) *Lbd_device {

	/* This function shouldn't exist, we should always create a device from a catalog entry
	but there are three places it is used:
	1) destroy block device, which is going away in favor of shutdown device
	2) get storage status, maybe we can fix this to use a catentry later.
	3) add new catentry. this is a problem, because first time init of a backing store
	sets calculated device information, from which the catentry is created
	so we may need to keep this around, because low level calculating things should know
	about devices (to update with the calculated values not catentries.  */
	var device Lbd_device

	device.Device_name = device_name
	device.Size = size
	device.Local_storage_file = local_storage_file
	device.Directio = directio
	device.Sync = sync

	device.Alignment = alignment

	device.Stree_value_size = stree_value_size
	device.Stree_calculated_node_size = calculated_stree_node_size
	device.Additional_nodes_per_block = additional_nodes_per_block

	device.Mount = mount
	device.Mountpoint = mountpoint

	device.stree = nil
	device.storage = nil

	/* for testing */
	device.device_ramdisk = device_ramdisk
	device.stree_ramdisk = stree_ramdisk

	return &device
}

func (this *Lbd_lib) device_startup(device *Lbd_device, force bool, data_pipeline *list.List) tools.Ret {
	/* some commands require a device definition without actually turning it on,
	   so startup is separate. */
	var ret tools.Ret
	ret, device.stree, device.storage = this.make_local_storage(device, force, data_pipeline)

	return ret
}

func (this *Lbd_lib) device_shutdown(device *Lbd_device) tools.Ret {
	// the only thing to shut down related to the device is the backing storage

	var ret = this.shutdown_local_storage(device)
	if ret != nil {
		this.log.Error("Error shutting down backing storage, error: ", ret.Get_errmsg())
	}
	return nil
}

func (this *Lbd_lib) make_file_store_aligned(device *Lbd_device, stree_block_size uint32) (tools.Ret, *stree_v_lib.File_store_aligned) {

	var alignment = device.Alignment // PHYSICAL_BLOCK_SIZE // 4k will use 8k per block because of our stree block header pushes the whole node size to a bit over 4k

	// if directio is set alignment must be % PHYSICAL_BLOCK_SIZE == 0, or reads and writes will fail.
	if device.Directio {
		if alignment == 0 {
			alignment = PHYSICAL_BLOCK_SIZE
			device.Alignment = alignment // force caller to get this update
		}

		if alignment%PHYSICAL_BLOCK_SIZE != 0 {
			return tools.Error(this.log, "your alignment must fall on a ", PHYSICAL_BLOCK_SIZE, " boundary if directio is on. ",
				"alignment: ", alignment, " % ", PHYSICAL_BLOCK_SIZE, " is ", alignment%PHYSICAL_BLOCK_SIZE), nil
		}
	} else {
		if alignment == 0 {
			alignment = stree_block_size
			device.Alignment = alignment // same thing here, if they didn't specify alignment, we tell the device what it should be
		}
	}

	/* first we have to see if we're doing directio or default io path so we can inject that into the
	   filestore aligned object */
	var iopath stree_v_lib.File_store_io_path
	if device.Directio {
		iopath = stree_v_lib.New_file_store_io_path_directio()
	} else {
		iopath = stree_v_lib.New_file_store_io_path_default()
	}

	/* so the backing physical store for the stree is the block device or file passed... */
	var fstore *stree_v_lib.File_store_aligned = stree_v_lib.New_File_store_aligned(this.log,
		device.Local_storage_file, uint32(stree_block_size), uint32(alignment),
		device.Additional_nodes_per_block, iopath)

	return nil, fstore
}

func (this *Lbd_lib) get_init_size_values(device *Lbd_device) (key_length uint32, value_length_out uint32,
	additional_nodes_per_block_out uint32, key_type_out string, value_type_out []byte) {

	/* get these from cmd line options or use hardcoded defaults. */

	// this is the size of the key that the thing storing the key makes
	var KEY_LENGTH uint32 = zosbd2_stree_v_storage_mechanism.Get_key_length()

	// this is how much data you can store in one node (mother or offspring), (we add a header, this is just the data size)
	var stree_node_value_size uint32 = device.Stree_value_size                // PHYSICAL_BLOCK_SIZE
	var additional_nodes_per_block uint32 = device.Additional_nodes_per_block // you always get one node, this is how many ADDITIONAL nodes you want. this is how many nodes there are in one block referred to by a single key minus one, because you always get one.

	/* this is how many total bytes you can store in a tree entry. so if you make the single node size 4k
	and you add 63 additional nodes, that's 64 4k blocks which is 256k per one keyed tree entry.
	if we compress that down, we can get it to store only 4k minimum, because that's the smallest single node
	size we can store. */
	var VALUE_LENGTH uint32 = stree_node_value_size
	/* if we ever see this in on disk then there's a bug. we can probably remove all this default stuff
	because we only had to make it for the java version where we had to get the generic type to give us a default
	value and key because only it could make data of the generic type. In our case we have strings for keys and
	a byte array for data always so we don't really need default values for anything. */
	var key_type string = "defkey"
	var value_type []byte = []byte("defvalue")
	return KEY_LENGTH, VALUE_LENGTH, additional_nodes_per_block, key_type, value_type
}

func (this *Lbd_lib) Make_stree(device *Lbd_device) (tools.Ret, *stree_v_lib.Stree_v) {

	/* we can not make an stree if it is uninitialized, the create is done elsewhere when the
	   backing store is added to the catalog. so by the time we get here, it must be
		 good already. */

	var key_length, value_length, additional_nodes_per_block, key_type, value_type = this.get_init_size_values(device)

	/* this is how much data in a single unit stree is going to ask the backing store to store.
	it's the size of the actual block of data we want to store plus the stree header,
	this is also called the stree_block_size */
	var ret, stree_calculated_node_size = stree_v_lib.Calculate_block_size(this.log, key_type, value_type, key_length, value_length, additional_nodes_per_block)
	if ret != nil {
		return ret, nil
	}

	if device.Stree_calculated_node_size != 0 {
		if device.Stree_calculated_node_size != stree_calculated_node_size {
			return tools.Error(this.log, "sanity failure, somebody has already set the stree calculated node size (to ",
				device.Stree_calculated_node_size, ") and it should be calculated here or set to ",
				stree_calculated_node_size), nil
		}
	}

	device.Stree_calculated_node_size = stree_calculated_node_size // this is always calculated, never set by caller

	var store stree_v_interfaces.Stree_v_backing_store_interface = nil

	if device.stree_ramdisk {
		// if you want to test using memory as backing the stree, do this.
		var mstore *stree_v_lib.Memory_store = stree_v_lib.New_memory_store(this.log)
		mstore.Init()
		store = mstore
	} else {
		var fstore *stree_v_lib.File_store_aligned
		ret, fstore = this.make_file_store_aligned(device, stree_calculated_node_size)
		if ret != nil {
			return ret, nil
		}
		store = fstore
	}
	var s *stree_v_lib.Stree_v = stree_v_lib.New_Stree_v(this.log, store, key_length, value_length,
		additional_nodes_per_block, stree_calculated_node_size, "", []byte(""))

	return nil, s
}

func (this *Lbd_lib) Start_stree(s *stree_v_lib.Stree_v, force bool) tools.Ret {
	/* start up the stree, check to make sure it is initted first */
	var ret, is_initted = s.Is_initialized()
	if ret != nil {
		return ret
	}
	if is_initted == false {
		return tools.ErrorWithCode(this.log, int(syscall.ENODATA), "backing store is uninitialized.")
	}

	ret = s.Startup(force)
	if ret != nil {
		return ret
	}
	return nil
}

func (this *Lbd_lib) shutdown_local_storage(device *Lbd_device) tools.Ret {
	if device.stree == nil {
		return nil
	}
	var ret = device.stree.Shutdown()
	if ret != nil {
		this.log.Error("Unable to shutdown backing storage, error: ", ret.Get_errmsg())
	}
	device.stree = nil
	return ret
}

func (this *Lbd_lib) make_local_storage(device *Lbd_device, force bool, data_pipeline *list.List) (tools.Ret,
	*stree_v_lib.Stree_v, zosbd2interfaces.Storage_mechanism) {

	var z zosbd2interfaces.Storage_mechanism = nil
	var stree *stree_v_lib.Stree_v = nil
	if device.device_ramdisk {
		/* test with ramdisk instead of stree */
		z = storage.New_ramdiskstorage(this.log, PHYSICAL_BLOCK_SIZE)
	} else {
		var ret tools.Ret
		ret, stree = this.Make_stree(device)
		if ret != nil {
			return ret, nil, nil
		}
		ret = this.Start_stree(stree, force)
		if ret != nil {
			return ret, nil, nil
		}
		/* now that we have an stree_v, we pass that to the zosbd2_backing_store */
		z = zosbd2_stree_v_storage_mechanism.New_zosbd2_storage_mechanism(this.log, stree, data_pipeline)
	}

	return nil, stree, z
}

func (this *Lbd_lib) get_active_device_map() (tools.Ret, map[string]zosbd2cmdlib.Device_status) {

	/* the keys of the returned map will be in lower case. */
	var kmod = zosbd2cmdlib.New_kmod(this.log)

	// open the control device
	var control_device_fd *os.File
	var ret tools.Ret
	ret, control_device_fd = kmod.Open_bd(this.control_device)
	if ret != nil {
		return tools.Error(this.log, "Unable to get status, can't open control device: ", this.control_device, " error: ", ret.Get_errmsg()), nil
	}
	defer func() {
		ret = kmod.Close_bd(control_device_fd)
		if ret != nil {
			this.log.Error("Error closing control device: ", this.control_device, " error: ", ret.Get_errmsg())
		}
	}()

	/* get the map of devices statuses */
	var map_of_devices map[string]zosbd2cmdlib.Device_status
	ret, map_of_devices = kmod.Get_devices_status_map(control_device_fd)
	if ret != nil {
		return ret, nil
	}

	return nil, map_of_devices

}

func (this *Lbd_lib) device_status() tools.Ret {
	/* kmod does all the heavy lifting here, just get the map of structs from it
	   and display it in json. */

	var ret tools.Ret
	var map_of_devices map[string]zosbd2cmdlib.Device_status
	ret, map_of_devices = this.get_active_device_map()
	if ret != nil {
		return ret
	}
	bytesout, err := json.MarshalIndent(map_of_devices, "", " ")
	if err != nil {
		return tools.Error(this.log, "unable to marshal device status information into json")
	}
	fmt.Println(string(bytesout))

	return nil
}

func (this *Lbd_lib) destroy_all_block_devices() tools.Ret {
	/* all the heavy lifting here is done by kmod */

	var kmod = zosbd2cmdlib.New_kmod(this.log)

	// open the control device
	var control_device_fd *os.File
	var ret tools.Ret
	ret, control_device_fd = kmod.Open_bd(this.control_device)
	if ret != nil {
		return tools.Error(this.log, "Unable to destroy all block devices, can't open control device: ",
			this.control_device, " error: ", ret.Get_errmsg())
	}

	defer func() {
		ret = kmod.Close_bd(control_device_fd)
		if ret != nil {
			this.log.Error("Error closing control device: ", this.control_device, " error: ", ret.Get_errmsg())
		}
	}()

	this.log.Debug("destroying all block devices")
	/* just call kmod destroy all */
	ret = kmod.Destroy_all_block_devices(control_device_fd)
	if ret != nil {
		return tools.Error(this.log, "Unable to destroy all block devices, error: ", ret.Get_errmsg())
	}

	return nil
}

func (this *Lbd_lib) destroy_block_device(device *Lbd_device) tools.Ret {
	/* all the heavy lifting here is done by kmod */

	// var device_name = flag.Arg(1)
	if len(device.Device_name) == 0 {
		return tools.Error(this.log, "device name required to destroy block device")
	}

	var kmod = zosbd2cmdlib.New_kmod(this.log)

	// open the control device
	var control_device_fd *os.File
	var ret tools.Ret
	ret, control_device_fd = kmod.Open_bd(this.control_device)
	if ret != nil {
		return tools.Error(this.log, "Unable to destroy all block devices, can't open control device: ",
			this.control_device, " error: ", ret.Get_errmsg())
	}

	defer func() {
		ret = kmod.Close_bd(control_device_fd)
		if ret != nil {
			this.log.Error("Error closing control device: ", this.control_device, " error: ", ret.Get_errmsg())
		}
	}()

	this.log.Debug("destroying block device: ", device.Device_name)
	/* just call kmod destroy all */
	ret = kmod.Destroy_block_device_by_name(control_device_fd, device.Device_name)
	if ret != nil {
		return tools.Error(this.log, "Error destroying block device for: ", device.Device_name, " error: ", ret.Get_errmsg())
	}

	return nil
}

func (this *Lbd_lib) storage_status(device *Lbd_device) tools.Ret {
	/* get the json blob from storage, and print it out */
	var key_length, value_length, additional_nodes_per_block, key_type, value_type = this.get_init_size_values(&Lbd_device{})

	var ret, block_size = stree_v_lib.Calculate_block_size(this.log, key_type, value_type, key_length, value_length, additional_nodes_per_block)
	if ret != nil {
		return ret
	}

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

	ret = fstore.Load_header_and_check_magic(false)
	if ret != nil {
		return ret
	}

	// var m map[string]string = make(map[string]string)
	// m["error"] = "backing store " + device.Local_storage_file + " has not been initialized, no status to report."

	// bytesout, err := json.Marshal(m)
	// if err != nil {
	// 	return tools.Error(this.log, "unable to marshal backing store information into json")
	// }
	// fmt.Println(string(bytesout))
	// return nil

	var json string
	ret, json = fstore.Get_store_information()
	if ret != nil {
		return ret
	}
	fmt.Println(json)
	return nil
}

func (this *Lbd_lib) process_pipeline_init_last_chance(data_pipeline *list.List,
	device *Lbd_device) tools.Ret {
	// run through each item in the pipeline and call process params on it.
	for item := this.data_pipeline.Front(); item != nil; item = item.Next() {
		var itemval = item.Value
		/* So I found out about this whole casting-panicing-on-nil thing. it turns out if you
		take one return parameter and the thing your casting is nil it will panic, but if
		you take two return parameters, the second ok one is what you can check instead of
		having it panic on you. */
		var pipline_element, ok = itemval.(zosbd2interfaces.Data_pipeline_element)
		if ok && pipline_element != nil {
			var ret = pipline_element.Process_device(device)
			if ret != nil {
				return ret
			}
		}
	}
	return nil
}

func (this *Lbd_lib) run_block_device(device *Lbd_device, force bool, data_pipeline *list.List, dragons bool) tools.Ret {

	/* 1/22/2022 I would like to detach this from the terminal since it blocks, except in go, you can't.
	   threads make it impossible to fork, so you have to forkexec which means starting-anew
		 which means we have to do it up front before we do anything. I'd like to detach after
		 I've gotten through most of the hard part so I can report an error if it doesn't work.
		 holding off on this for now. */

	/* 3/14/2022 since there is no way to fork in go, we can only fork/exec, or start a new process.
	   we want to verify up front that as much stuff is working as possible and if all systems are go
		 start a child process to do all the same stuff with the same parameters, but actually
		 handle the block device callback. I'm torn between making it an environment variable for the
		 child process, which is hacky, or a command line param which is visible to the user.
		 I think I'll go with command line parameter and scare them away with dragons. */

	if dragons == false {
		this.log.Info("starting validation phase for: ", device.Device_name)
	}

	if device.Size%PHYSICAL_BLOCK_SIZE != 0 { // xxxz move these to catalog add
		return tools.ErrorWithCode(this.log, int(syscall.EINVAL), "block device size is not a multiple of ", PHYSICAL_BLOCK_SIZE)
	}

	if device.Size < ONE_MEG {
		return tools.ErrorWithCode(this.log, int(syscall.EINVAL), "block device size must be at least ", ONE_MEG)
	}

	var number_of_block_device_blocks uint64 = device.Size / uint64(PHYSICAL_BLOCK_SIZE)

	var ret = this.device_startup(device, force, data_pipeline)
	if ret != nil {
		return tools.Error(this.log, "can not start up block device, error from backing store:", ret.Get_errmsg())
	}

	defer func() {
		ret = this.device_shutdown(device)
		if ret != nil {
			this.log.Error("Error shutting down backing storage, error: ", ret.Get_errmsg())
		}
	}()

	/* 5/7/2022 there really is no great place to do this. with the advent of the kompressor
	   we need a way to give the pipeline elements one last shot at initialization now
		 that we know everything there is to know about the backing store and the block device
		 and the catalog entry definition. ie: the kompressor needs to know the block size
		 and that is not available when we start up the application and first make the
		 kompressor. so we do it here. */

	ret = this.process_pipeline_init_last_chance(data_pipeline, device)
	if ret != nil {
		return ret
	}

	var kmod = zosbd2cmdlib.New_kmod(this.log)

	// open the control device
	var control_device_fd *os.File
	ret, control_device_fd = kmod.Open_bd(this.control_device)
	if ret != nil {
		return tools.Error(this.log, "Unable to create block device: ", device.Device_name, ", can't open control device: ",
			this.control_device, " error: ", ret.Get_errmsg())
	}

	defer func() {
		ret = kmod.Close_bd(control_device_fd)
		if ret != nil {
			this.log.Error("Error closing control device: ", this.control_device, " error: ", ret.Get_errmsg())
		}
	}()

	/* Create block device */
	var handle_id uint32
	ret, handle_id = kmod.Create_block_device(control_device_fd, device.Device_name, PHYSICAL_BLOCK_SIZE, number_of_block_device_blocks, TIMEOUT_IN_SECONDS)
	if ret != nil {
		return tools.Error(this.log, "Unable to create block device: ", device.Device_name, " error: ", ret.Get_errmsg())
	}

	if dragons {
		// start a goroutine to mount the block device if the catentry says to
		this.attempt_mount(device)

		var block_device_handler = zosbd2cmdlib.New_block_device_handler(this.log, &kmod, device.Device_name, device.storage, handle_id)

		/* go run the thing */
		ret = block_device_handler.Run()
		if ret != nil {
			/* if the device ran successfully we only get here when the block device was destroyed by an external
			   caller, hitting the destroy device ioctl. But if the block device handler failed, then likely nobody
			   cleaned up the device, so we should do it here. I know we used to have to the double clean problem
			   but that's when I was calling destroy all the time, not just on error.*/
			var ret2 = this.destroy_block_device(device)
			if ret2 != nil {
				this.log.Error("Error cleaning up block device after block device handler failed, err: " + ret2.Get_errmsg())
			}
			return tools.Error(this.log, "Error running block device handler for: ", device.Device_name, " error: ", ret.Get_errmsg())
		}
		return nil // the actual running of the device completed successfully
	}
	// non-dragon mode
	/* First shutdown the backing store, this will get called again on defer, but it's designed to quietly be called twice. */
	ret = this.device_shutdown(device)
	if ret != nil {
		this.log.Error("Error shutting down backing storage, error: ", ret.Get_errmsg())
	}
	/* then undo the block device */
	var ret2 = this.destroy_block_device(device)
	if ret2 != nil {
		this.log.Error("Error cleaning up block device after block device load test, err: " + ret2.Get_errmsg())
	}
	this.log.Info("finished validation phase for: ", device.Device_name)
	/* at this point the only thing not closed is the file handle to the zosbd2 control device, and that's fine to have
	that open while we shell. */
	this.log.Info("starting device: ", device.Device_name)
	// shell to the real deal with dragons
	var executable, err = os.Executable()
	if err != nil {
		return tools.Error(this.log, "unable to determine block device binary to execute: ", err.Error())
	}

	var cmd = exec.Command(executable, CMD_CATALOG, SUB_CMD_CATALOG_START, "--"+TXT_DEVICE_NAME, device.Device_name, "--"+TXT_DRAGONS)
	err = cmd.Start() // and away it goes.
	if err != nil {
		return tools.Error(this.log, "unable to start background process ", executable, " err: ", err.Error())
	}
	return nil
}
