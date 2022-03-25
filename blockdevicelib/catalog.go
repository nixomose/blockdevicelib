/* Package blockdevicelib has a comment to make the linter happy. */
package blockdevicelib

import (
	"container/list"
	"encoding/json"
	"fmt"

	"github.com/nixomose/stree_v/stree_v_lib/stree_v_lib"
	"github.com/nixomose/zosbd2goclient/zosbd2cmdlib"

	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/nixomose/nixomosegotools/tools"
)

type Catalog_entry struct {
	// settings for the device
	Device_name        string
	Size               uint64 // in bytes
	Local_storage_file string

	// backing store sizing information
	// do you want to use O_DIRECT
	Directio bool
	// do you want to use O_SYNC
	Sync bool

	/* if using directio this must be a multiple of 4k for reads and writes to work. you can make it largers but you'd just
	    be wasting space. If not using directio you can make this the stree_block_size to perfectly line up stree nodes
			back to back with zero waste. As this will create unaligned reads and writes to the back end it will suffer a performance
			penalty, but not using directio will get you page caching so it actually goes faster than directio. until we
			start flushing. */
	Alignment uint32

	/* this is the number of bytes of data the user wants to be able to store in a mother node. the calculated
	size is the actual size we're going to create a node size of which includes the header based on the key size
	and the value size and other mother node header information */
	Node_value_size_bytes uint32

	/* this is the size of a single node in stree, for example a mother node or an offpsring node. This is the smallest
	   size an stree block of data can be stored in, if for example you were to compress it. */
	Node_calculated_size_bytes uint32

	/* you always get one mother node to store user data in (of size node_value_size_bytes) this is how many additional nodes you
	   want to use to make up an stree tree node entry.
	 	 (additional_nodes_per_block + 1) * node_value_size_bytes is the largest amount of data you can store in an stree tree node.
		 if it were to compress well, you would actually only need a minimum of 1 node (of node_value_size_bytes bytes) to store it in
		 if for example it compressed well, or you only wrote a little bit of data into it. */
	Additional_nodes_per_block uint32 //

	Mount      bool   // should we try and mount after "catalog start", and similarly unmount on "catalog stop"
	Mountpoint string // where we should mount it

	Exclude_from_start_all bool // by default we include all catalog entries when we say start all

}

func New_catalog_entry_from_device(device *Lbd_device) Catalog_entry {
	var entry Catalog_entry = Catalog_entry{}
	entry.Device_name = device.Device_name
	entry.Size = device.Size
	entry.Local_storage_file = device.Local_storage_file
	entry.Directio = device.Directio
	entry.Sync = device.Sync
	entry.Alignment = device.Alignment
	entry.Node_value_size_bytes = device.Stree_value_size
	entry.Node_calculated_size_bytes = device.Stree_calculated_node_size
	entry.Additional_nodes_per_block = device.Additional_nodes_per_block
	entry.Mount = device.Mount
	entry.Mountpoint = device.Mountpoint
	entry.Exclude_from_start_all = device.Exclude_from_start_all
	return entry
}

type Catalog_list struct {
	Device_list map[string]*Catalog_entry // map of case preserved device_name to device definition.
}

type Catalog struct {
	log *tools.Nixomosetools_logger

	catalog_file string
	catalog_list *Catalog_list
}

func New_catalog(log *tools.Nixomosetools_logger, catalog_file string) *Catalog {

	var ret Catalog = Catalog{}
	ret.log = log
	ret.catalog_file = catalog_file
	ret.catalog_list = &Catalog_list{Device_list: make(map[string]*Catalog_entry)}
	return &ret
}

func New_catalog_list() *Catalog_list {

	var ret = Catalog_list{}
	ret.Device_list = make(map[string]*Catalog_entry)
	return &ret
}

func (this *Catalog) Read_catalog(cat *Catalog) tools.Ret {

	// reload from disk
	if cat.catalog_list == nil {
		cat.catalog_list = New_catalog_list()
	}

	var metadata, err = toml.DecodeFile(cat.catalog_file, &cat.catalog_list)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // this is fine for first time in
		}
		var m, err2 = json.MarshalIndent(metadata, "", "  ")
		if err2 != nil {
			m = []byte("unable to display metadata.")
		}
		return tools.Error(this.log, "Unable to read catalog file: ", cat.catalog_file,
			" err: ", err, ", metadata: ", string(m))
	}

	/* amazingly, the list loads into the catalog_list correctly. */
	return nil

}

func (this *Catalog) Write_catalog() tools.Ret {

	f, err := os.Create(this.catalog_file)
	if err != nil {
		// failed to create/open the file
		return tools.Error(this.log, "unable to create catalog file: ", this.catalog_file, " err: ", err)
	}
	if err := toml.NewEncoder(f).Encode(this.catalog_list); err != nil {
		// failed to encode
		return tools.Error(this.log, "unable to write catalog file: ", this.catalog_file, " err: ", err)
	}
	if err := f.Close(); err != nil {
		// failed to close the file
		return tools.Error(this.log, "unable to close catalog file: ", this.catalog_file, " err: ", err)
	}
	return nil
}

/********************************************************************/
/*                       catalog handling                           */
/********************************************************************/

func (this *Lbd_lib) get_catalog_entry(cat *Catalog, device_name string) (tools.Ret, *Catalog_entry) {
	/* sift through the map so we can match lowercase always but they can keep their
	   original case in the name in the catalog. */

	var ret = this.catalog.Read_catalog(cat)
	if ret != nil {
		return ret, nil
	}

	var lower_device_name = strings.ToLower(device_name)
	for k, v := range cat.catalog_list.Device_list {
		var lower_key = strings.ToLower(k)
		if lower_device_name == lower_key {
			return nil, v
		}
	}
	return tools.ErrorWithCodeNoLog(this.log, int(syscall.ENOENT)), nil
}

func (this *Lbd_lib) delete_catalog_entry(cat *Catalog, device_name string) tools.Ret {
	/* sift through the map so we can match lowercase always but they can keep their
	   original case in the name in the catalog, delete the entry from the map
		 and write to disk. */

	var ret = this.catalog.Read_catalog(cat)
	if ret != nil {
		return ret
	}

	var lower_device_name = strings.ToLower(device_name)
	for k := range cat.catalog_list.Device_list {
		var lower_key = strings.ToLower(k)
		if lower_device_name == lower_key {
			delete(cat.catalog_list.Device_list, k)
			return cat.Write_catalog()
		}
	}
	return tools.Error(this.log, "device ", device_name, " not found to delete")
}

func (this *Lbd_lib) add_to_catalog(cat *Catalog, device *Lbd_device) tools.Ret {
	/* add to the catalog if it's not there.
	     if it is already there but the values are the same, it's not an error.
			 if it's already there and any values are different, return an error. */

	//	var cat Catalog = *New_catalog(l.log, l.catalog_file)
	// look up by default name, that's always our key. case insensitive
	// race condition here for adding right after adding.
	var ret, _ = this.get_catalog_entry(cat, device.Device_name)
	if ret != nil {
		if ret.Get_errcode() != int(syscall.ENOENT) {
			return ret // some other worse error. not found is okay
		} else {

			// doesn't exist, add it
			// var newcatentry = New_catalog_entry(device.Device_name, device.Size, device.Local_storage_file, device.Directio, device.Sync,
			// 	device.Alignment, device.Stree_value_size, device.Stree_calculated_node_size, device.Additional_nodes_per_block, device.Mount, device.Mountpoint)
			var newcatentry = New_catalog_entry_from_device(device)

			// add new catalog entry.
			cat.catalog_list.Device_list[device.Device_name] = &newcatentry
			return cat.Write_catalog()
		}
	}
	return tools.Error(this.log, "sanity failure catalog entry shouldn't exist, but it does. not rewriting.")
	// this can't really happen, because we checked above that it doesn't exist yet
	// var same bool = true
	// if catentry != nil {
	// 	// compare values, if they're different return error, if they're the same, nothing to do, they're the same.
	// 	if catentry.Size != device.Size {
	// 		same = false
	// 	}
	// 	if catentry.Local_storage_file != device.Local_storage_file {
	// 		same = false
	// 	}
	// 	if catentry.Directio != device.Directio {
	// 		same = false
	// 	}
	// 	if catentry.Sync != device.Sync {
	// 		same = false
	// 	}
	// 	if catentry.Alignment != device.Alignment {
	// 		same = false
	// 	}
	// 	if catentry.Node_value_size_bytes != device.Stree_value_size {
	// 		same = false
	// 	}
	// 	if catentry.Node_calculated_size_bytes != device.Stree_calculated_node_size {
	// 		same = false
	// 	}
	// 	if catentry.Additional_nodes_per_block != device.Additional_nodes_per_block {
	// 		same = false
	// 	}
	// 	if catentry.Mount != device.Mount {
	// 		same = false
	// 		// mountpoint is optional so only check path if there is a mount
	// 		if catentry.Mountpoint != device.Mountpoint {
	// 			same = false
	// 		}
	// 	}

	// } // if there is an existing cat entry

	// if same == false {
	// 	// print out both and let them see what's different
	// 	type printer struct {
	// 		msg string
	// 		c1  *Catalog_entry
	// 		d1  *Lbd_device
	// 	}
	// 	var msg printer = printer{}
	// 	msg.msg = "existing catalog entry for " + device.Device_name + " does not match device specification"
	// 	msg.c1 = catentry
	// 	msg.d1 = device

	// 	var bytesout, err = json.MarshalIndent(msg, "", " ")
	// 	if err != nil {
	// 		return tools.Error(this.log, "unable to marshal catalog entry information into json, err: ", err)
	// 	}
	// 	var msgstring = fmt.Sprint(string(bytesout))

	// 	return tools.Error(this.log, msgstring)

	// } // if they're not the same

	// they are the same, nothing to do.
	// return nil
}

func (this *Lbd_lib) dump_catentry(catentry *Catalog_entry) tools.Ret {

	var bytesout, err = json.MarshalIndent(catentry, "", " ")
	if err != nil {
		return tools.Error(this.log, "unable to marshal catalog entry information into json, err: ", err)
	}
	var msgstring = fmt.Sprint(string(bytesout))
	fmt.Println(msgstring)

	return nil
}

func (this *Lbd_lib) dump_catentry_list(catentrylist []*Catalog_entry) tools.Ret {

	var bytesout, err = json.MarshalIndent(catentrylist, "", " ")
	if err != nil {
		return tools.Error(this.log, "unable to marshal catalog entry list information into json, err: ", err)
	}
	var msgstring = fmt.Sprint(string(bytesout))
	fmt.Println(msgstring)

	return nil
}

func (this *Lbd_lib) catalog_list_all(cat *Catalog) tools.Ret {
	/* output the device information for all devices in the catalog */

	var ret = this.catalog.Read_catalog(cat)
	if ret != nil {
		return ret
	}

	var collection = make([]*Catalog_entry, 0)
	for _, catentry := range cat.catalog_list.Device_list {
		collection = append(collection, catentry)
	}

	this.dump_catentry_list(collection)

	return nil
}

func (this *Lbd_lib) catalog_list_device(cat *Catalog, device_name string) tools.Ret {
	/* output the device information for all devices in the catalog */

	var ret, catentry = this.get_catalog_entry(cat, device_name)
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENOENT) {
			return tools.Error(this.log, "device: ", device_name, " not found")
		}
		return ret
	}
	this.dump_catentry(catentry)
	return nil
}

func (this *Lbd_lib) catalog_add(cat *Catalog, device *Lbd_device) tools.Ret {
	/* add this device to the catalog if not already there. */

	var ret, _ = this.get_catalog_entry(cat, device.Device_name)
	if ret != nil {
		if ret.Get_errcode() != int(syscall.ENOENT) {
			return ret
		}
	} else {
		return tools.Error(this.log, "cannot add ", device.Device_name, ", device name already exists in the catalog.")
	}

	/* Now try and initialize the backing store. if it is not unitialized, fail. */
	var stree *stree_v_lib.Stree_v = nil
	ret, stree = this.Make_stree(device)
	if ret != nil {
		return ret
	}

	var initted bool = true
	ret, initted = stree.Is_initialized()
	if ret != nil {
		/* if it's a block device, and it's not found, then it's not a block device,
		   it's a file that doesn't exist. */
		if ret.Get_errcode() == int(syscall.ENOENT) {
			initted = false
		} else {
			return ret
		}
	}
	if initted {
		return tools.Error(this.log, "can not create catalog entry, backing store contains data.")
	}
	/* so we're clear for takeoff, the backing store is there and uninitialzied it.
	   init it, and add this device definition to the catalog. */
	ret = stree.Init()
	if ret != nil {
		return ret
	}

	/* we have to make the device first, not the catalog entry, because the act of creating the stree
	     and initializing the backing store is what fills out the calculated things like stree byte size and
			 the alignment. So we make a lbd_device, let all the entries get filled out in the device, and then
	     we make a catentry out of it, to store in the catalog below. */

	ret = stree.Shutdown() // don't need it anymore
	if ret != nil {
		return ret
	}

	// now do the local housekeeping
	return this.add_to_catalog(this.catalog, device)
}

func (this *Lbd_lib) catalog_delete(cat *Catalog, device_name string) tools.Ret {
	/* delete the backing store if it's a file and remove the entry from the catalog.
	   even if it's running. */

	/* I guess we should first make sure it's not running. Let's do that. */

	var ret tools.Ret
	var map_of_devices map[string]zosbd2cmdlib.Device_status
	ret, map_of_devices = this.get_active_device_map()
	if ret != nil {
		return ret
	}
	var lower_device_name = strings.ToLower(device_name)
	var _, ok = map_of_devices[lower_device_name]
	if ok != false {
		return tools.Error(this.log, "block device: ", device_name, " can not be deleted while it is started")
	}
	/* of course there's a race condition here, but let's assume they don't start and delete frequently.  */
	/* so if we're here there's no active block device, all we have to do is remove it
	from the catalog and if the backing store is a file, delete the file. */
	/* I suppose for completeness we should zero out the first block so we could re init again
	   with no grief */
	/* let's do that first. */
	var catentry *Catalog_entry
	ret, catentry = this.get_catalog_entry(cat, device_name)
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENOENT) {
			return tools.Error(this.log, "device: ", device_name, " not found")
		}
		return ret
	}

	var device = this.New_block_device_from_catalog_entry(catentry)

	/* you can only delete real backing store storage items, ie you
	can't delete a ramdisk, because only I should be doing that. */

	/* if there is a problem starting up the device, perhaps because a previous
	delete half failed, then we should let it go and just remove from the catalog.
	cleaning up the mess will have to be a manual process of running other wipe
	commands and things like that. delete should delete out of the catalog
	so you can add again. */

	ret = this.device_startup(device, true, &list.List{}) // startup even if dirty, no pipeline
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENODATA) {
			/* if it is not initialized, nothing to wipe, the storage object
			didn't get created so we can't call dispose on it, but we can at least
			continue and delete the entry from the catalog. */
			this.log.Info("backing storage is not initialized, can not wipe it.")
		} else {
			this.log.Info("error validating backing storage, error: ", ret.Get_errmsg())
		}
	} else {
		/* so now we have the zos device and the stree in the device.
		we can't use the zos device because that makes accessing the innards of the stree
		impossible, so we go right after the stree. */
		ret = device.stree.Wipe()
		if ret != nil {
			return ret
		}
		// dispose calls shutdown so the device or file is closed before it is deleted
		ret = device.stree.Dispose()
		if ret != nil {
			return ret
		}
	}

	ret = this.delete_catalog_entry(this.catalog, device.Device_name)
	if ret != nil {
		return ret
	}
	this.log.Info("device ", device_name, " has been removed from the catalog and the backing data destroyed")
	return nil
}

func (this *Lbd_lib) catalog_shutdown_all(cat *Catalog) tools.Ret {
	/* cleanly shutdown all devices in the catalog that aren't marked exclude
	simply by clling shutdown device on each one. if there's an error on one
	don't stop doing the others.
	probably we should only bother for the ones that are up.
	so let's go through the active device list not the catalog */

	var ret tools.Ret
	var map_of_devices map[string]zosbd2cmdlib.Device_status
	ret, map_of_devices = this.get_active_device_map()
	if ret != nil {
		return ret
	}

	var any_failed bool = false
	var failed_device_list string = ""
	for device_name := range map_of_devices {
		this.log.Info("calling shutdown on device: ", device_name)
		var ret = this.catalog_shutdown_device(cat, device_name)
		if ret != nil {
			any_failed = true
			failed_device_list = failed_device_list + " " + device_name
		}

	}
	if any_failed {
		return tools.Error(this.log, "the following devices failed to shutdown cleanly:", failed_device_list)
	}
	return nil
}

func (this *Lbd_lib) catalog_shutdown_device(cat *Catalog, device_name string) tools.Ret {
	/* cleanly shutdown this device, unmounting the filesystem if the catalog entry
	is marked mount=true. The easy part here is that we're a separate process so we can
	just call unmount synchronously. */

	/* can't shutdown something that's not running, check that first. */

	var ret tools.Ret
	var map_of_devices map[string]zosbd2cmdlib.Device_status
	ret, map_of_devices = this.get_active_device_map()
	if ret != nil {
		return ret
	}
	var lower_device_name = strings.ToLower(device_name)
	var _, ok = map_of_devices[lower_device_name]
	if ok == false {
		return tools.Error(this.log, "can't shutdown block device: ", device_name, " not found.")
	}

	var catentry *Catalog_entry
	ret, catentry = this.get_catalog_entry(cat, device_name)
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENOENT) {
			return tools.Error(this.log, "device: ", device_name, " not found")
		}
		return ret
	}

	// the man page says this can not fail. :-)
	syscall.Sync()

	/* See if it is set to mount. if so, call unmount on it first, then shut the device down, */
	var device = this.New_block_device_from_catalog_entry(catentry)
	if catentry.Mount {
		ret = this.attempt_unmount(device)
		if ret != nil {
			return nil
		}
	}

	/* once unmount completes and we have synced, we can safely destroy the block device,
	   there should be no outstanding writes waiting to happen */
	ret = this.destroy_block_device(device)
	if ret != nil {
		return ret // it has already logged the error.
	}
	return nil
}

func (this *Lbd_lib) catalog_start_all(cat *Catalog, force bool, data_pipeline *list.List) tools.Ret {
	/* go through the catalog and call start on every device that isn't marked
	   exclude. */

	this.log.Info("starting all non excluded entries in the catalog.")
	var ret = this.catalog.Read_catalog(cat)
	if ret != nil {
		return ret
	}

	var any_failed bool = false
	var failed_device_list string = ""

	for device_name, catentry := range cat.catalog_list.Device_list {
		// forgot to actually check the exclude flag
		if catentry.Exclude_from_start_all {
			continue
		}
		ret = this.catalog_start_device(cat, device_name, force, data_pipeline, false, false, false)
		if ret != nil {
			any_failed = true
			failed_device_list = failed_device_list + " " + device_name
		}
	}
	if any_failed {
		return tools.Error(this.log, "the following devices failed to start cleanly:", failed_device_list)
	}

	return nil
}

func (this *Lbd_lib) catalog_start_device(cat *Catalog, device_name string, force bool,
	data_pipeline *list.List, device_ramdisk bool, stree_ramdisk bool, dragons bool) tools.Ret {
	/* start the block device. */

	/* this is basically what create block device used to do, except we just read the info out of the
	   catalog now, and we don't ever init anything, the backing store is initted at create time. */

	var ret, catentry = this.get_catalog_entry(cat, device_name)
	if ret != nil {
		if ret.Get_errcode() == int(syscall.ENOENT) {
			return tools.Error(this.log, "device: ", device_name, " not found")
		}
		return ret
	}

	/* check and see if the device is already running. race condition here of course. */
	/* of course there's a race condition here, but let's assume they don't start and start again or delete frequently.  */

	var map_of_devices map[string]zosbd2cmdlib.Device_status
	ret, map_of_devices = this.get_active_device_map()
	if ret != nil {
		return ret
	}
	var lower_device_name = strings.ToLower(device_name)
	var _, ok = map_of_devices[lower_device_name]
	if ok != false {
		return tools.ErrorWithCode(this.log, int(syscall.EALREADY), "block device: ", device_name, " is already started")
	}

	var device = this.New_block_device_from_catalog_entry(catentry)

	// override for testing if the user passed it in
	device.device_ramdisk = device_ramdisk
	device.stree_ramdisk = stree_ramdisk

	ret = this.run_block_device(device, force, data_pipeline, dragons)
	if ret != nil {
		return ret // it has already logged the error.
	}
	if dragons {
		this.log.Info("device: ", device.Device_name, " has been shutdown.")
	}
	return nil // they shutdown the device
}

func (this *Lbd_lib) attempt_mount(device *Lbd_device) {
	if device.Mount == false {
		return // nothing to do here
	}
	/* start a go routine to attempt to mount the requested mountpoint to this
	block device. We can actually try to mount it right now, since the device exists
	by the time we're called. if the handler hasn't gotten it's shit together yet
	that's fine, the mount call will just block until it does. */

	go this.attempt_mount_runner(device)
}

func (this *Lbd_lib) attempt_mount_runner(device *Lbd_device) {
	this.log.Info("attempting to mount ", TXT_DEVICE_PATH_PREFIX, device.Device_name+" on "+device.Mountpoint)
	var handle = exec.Command(TXT_MOUNT_CMD, TXT_DEVICE_PATH_PREFIX+device.Device_name, device.Mountpoint)
	var output, err = handle.CombinedOutput()
	if err != nil {
		this.log.Error("error executing mount command: ", err)
		this.log.Error(string(output))
		return
	}
	this.log.Info("mount command completed")
}

func (this *Lbd_lib) attempt_unmount(device *Lbd_device) tools.Ret {
	if device.Mount == false {
		return nil // nothing to do here
	}
	/* I was going to store this state somewhere, but the only place to do it was the catalog
	and that's not the best place for state, so instead, check and see if the mountpoint is mounted
	and if it is, unmount it. */
	var ret, mounted = tools.Is_mounted(this.log, device.Mountpoint)
	if ret != nil {
		return ret
	}

	if mounted == false {
		this.log.Info("mount point ", device.Mountpoint, " not mounted, skipping unmount.")
		return nil
	}
	// if this fails we don't want to detach the block device, let them try again, leave everything running
	this.log.Info("attempting to unmount ", TXT_DEVICE_PATH_PREFIX, device.Device_name+" from "+device.Mountpoint)
	var handle = exec.Command(TXT_UMOUNT_CMD, device.Mountpoint)
	var output, err = handle.CombinedOutput()
	if err != nil {
		tools.Error(this.log, string(output))
		return tools.Error(this.log, "error executing umount command: ", err)
	}
	this.log.Info("umount command completed")
	return nil
}

/********************************************************************/
/*                  config setting handling                         */
/********************************************************************/

func (this *Lbd_lib) set_catalog_entry_exclude_device(cat *Catalog, device_name string, exclude bool) tools.Ret {

	var ret = this.catalog.Read_catalog(cat)
	if ret != nil {
		return ret
	}

	// we can't just do a map lookup because the device name is case preserved
	var lower_device_name = strings.ToLower(device_name)
	for k, entry := range cat.catalog_list.Device_list {
		var lower_key = strings.ToLower(k)
		if lower_device_name == lower_key {
			entry.Exclude_from_start_all = exclude
			return cat.Write_catalog()
		}
	}
	return tools.Error(this.log, "device ", device_name, " not found")
}
