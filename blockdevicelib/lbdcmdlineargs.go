// SPDX-License-Identifier: LGPL-2.1
// Copyright (C) 2021-2022 stu mark

package blockdevicelib

import (
	"container/list"
	"log"
	"log/syslog"

	"github.com/nixomose/nixomosegotools/tools"
	"github.com/nixomose/zosbd2goclient/zosbd2cmdlib/zosbd2interfaces"
	"github.com/spf13/cobra"
)

func (this *Lbd_lib) add_device_status(root_cmd *cobra.Command) {
	/* device status */
	var cmd_device_status = &cobra.Command{
		Use:   CMD_DEVICE_STATUS,
		Short: "display status of all block devices",
		Long:  `device-status will list all the active block devices and their configuration settings.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			this.device_status()
		},
	}
	root_cmd.AddCommand(cmd_device_status)
}

func (this *Lbd_lib) add_storage_status(root_cmd *cobra.Command) {
	/* storage status */
	var storage_file string
	var cmd_storage_status = &cobra.Command{
		Use:   CMD_STORAGE_STATUS,
		Short: "display definition of backing storage",
		Long:  `storage-status will display details of the layout of the backing storage.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			// this is the device definition, it only needs to be started for create
			var err error
			storage_file, err = cmd.Flags().GetString(TXT_STORAGE_FILE)
			if err != nil {
				tools.Error(this.log, err)
			} else {
				/* we just need enough default device settings so we can open the file and read the header
				   directio on allows us to read a big enough chunk to get the header. */
				var device = this.New_block_device("", 0, storage_file, true, false, PHYSICAL_BLOCK_SIZE, 0, 0, 0, false, "", false, false)
				this.storage_status(device)
			}
		},
	}
	cmd_storage_status.Flags().StringVarP(&storage_file, TXT_STORAGE_FILE, "t", "", "path of file or block device for backing storage")
	cmd_storage_status.MarkFlagRequired(TXT_STORAGE_FILE)
	root_cmd.AddCommand(cmd_storage_status)
}

func (this *Lbd_lib) add_destroy_block_device(root_cmd *cobra.Command) {
	/* destroy block device by name */
	// if you're defining a block device
	// this is in case of emergency. you should use catalogentry stop instead of this.,
	// you should shutdown a block device or delete a catalog entry.
	/* turns out we need to keep this for the case where they kill the userspace process
	there's no other way to remove the block device to get it into a workable state again. */
	var device_name string

	var cmd_destroy_block_device = &cobra.Command{
		Use:   CMD_DESTROY_BLOCK_DEVICE,
		Short: "destroy a block device by name",
		Long:  `this command will cause the block device to cleanly hang up on the userspace appliction servicing the named block device.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {

			// get all the required fields
			var err error
			device_name, err = cmd.Flags().GetString(TXT_DEVICE_NAME)
			if err != nil {
				tools.Error(this.log, err)
				return
			}
			var device = this.New_block_device(device_name, 0, "", false, false, 1, 0, 0, 0, false, "", false, false)
			this.destroy_block_device(device)
		},
	}
	cmd_destroy_block_device.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to create")
	cmd_destroy_block_device.MarkFlagRequired(TXT_DEVICE_NAME)
	root_cmd.AddCommand(cmd_destroy_block_device)
}

func (this *Lbd_lib) add_destroy_all_block_devices(root_cmd *cobra.Command) {
	var cmd_destroy_all_block_devices = &cobra.Command{
		Use:   CMD_DESTROY_ALL_BLOCK_DEVICES,
		Short: "destroy all block devices",
		Long:  `this command will cause all existing block devices to cleanly hang up on the userspace applictions servicing those block devices.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			this.destroy_all_block_devices()
		},
	}
	root_cmd.AddCommand(cmd_destroy_all_block_devices)
}

/* diagnostic commands */

func (this *Lbd_lib) add_diag_commands(root_cmd *cobra.Command) {

	var cmd_diag = &cobra.Command{
		Use:   CMD_DIAG,
		Short: "diagnostic tools",
		Long:  `this command will allow you do view detailed information about application's internal workings.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			tools.Error(this.log, "please specify a subcommand for diag")
		}}

	root_cmd.AddCommand(cmd_diag)

	this.add_dump(cmd_diag)
}

func (this *Lbd_lib) add_dump(cmd_diag *cobra.Command) {
	var cmd_dump = &cobra.Command{
		Use:   SUB_CMD_DUMP,
		Short: "dump the select content",
		Long:  `this command will allow you to see the details of a specific block or header.`,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			tools.Error(this.log, "please specify a subcommand for dump")
		}}

	cmd_diag.AddCommand(cmd_dump)

	this.add_dump_header(cmd_dump)       // structure of block 0
	this.add_dump_block_header(cmd_dump) // provided a block number display the structure of that block's header
	this.add_dump_block(cmd_dump)        // pretty print the contents of the provided block number
}

func (this *Lbd_lib) add_dump_header(root_cmd *cobra.Command) {
	var device_name string
	var cmd_dump_header = &cobra.Command{
		Use:   SUB_CMD_DUMP_HEADER,
		Short: "pretty print the contents of the backing store header",
		Long:  `this command will pretty print the contents of the backing store header.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			this.Dump_header(this.catalog, device_name)
		},
	}
	cmd_dump_header.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to dump the header of")
	cmd_dump_header.MarkFlagRequired(TXT_DEVICE_NAME)

	root_cmd.AddCommand(cmd_dump_header)
}

func (this *Lbd_lib) add_dump_block_header(root_cmd *cobra.Command) {
	var device_name string
	var cmd_dump_block_header = &cobra.Command{
		Use:   SUB_CMD_DUMP_BLOCK_HEADER,
		Short: "dump the contents of the header of the specified block",
		Long:  `this command will dump the contents of the header of the specified block.`,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err, block_num = tools.Stringtouint32(args[0])
			if err != nil {
				tools.Error(this.log, "error parsing command line for block number to dump: ", err)
				return
			}
			this.Dump_block_header(this.catalog, device_name, block_num)
		},
	}
	cmd_dump_block_header.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to dump the header of")
	cmd_dump_block_header.MarkFlagRequired(TXT_DEVICE_NAME)
	root_cmd.AddCommand(cmd_dump_block_header)
}

func (this *Lbd_lib) add_dump_block(root_cmd *cobra.Command) {
	var device_name string
	var cmd_dump_block = &cobra.Command{
		Use:   SUB_CMD_DUMP_BLOCK,
		Short: "dump the contents of the specified block",
		Long:  `this command will dump the contents of the specified block.`,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var err, block_num = tools.Stringtouint32(args[0])
			if err != nil {
				tools.Error(this.log, "error parsing command line for block number to dump: ", err)
				return
			}
			this.Dump_block(this.catalog, device_name, block_num)
		},
	}
	cmd_dump_block.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to dump the block of")
	cmd_dump_block.MarkFlagRequired(TXT_DEVICE_NAME)
	root_cmd.AddCommand(cmd_dump_block)
}

/* catalog commands */

func (this *Lbd_lib) add_catalog_commands(root_cmd *cobra.Command) {
	var cmd_catalog = &cobra.Command{
		Use:   CMD_CATALOG,
		Short: "list one or all of the devices defined in the catalog",
		Long:  `this command will list the specified or all of the existing block device definitions in the catalog.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			tools.Error(this.log, "please specify a subcommand for catalog")
		}}

	root_cmd.AddCommand(cmd_catalog)

	this.add_catalog_list(cmd_catalog)
	this.add_catalog_add(cmd_catalog)
	this.add_catalog_delete(cmd_catalog)

	this.add_start_device_from_catalog(cmd_catalog)
	this.add_stop_device_from_catalog(cmd_catalog) // clean shutdown (will try and unmount)

	this.add_catalog_set_commands(cmd_catalog)
}

func (this *Lbd_lib) add_catalog_list(root_cmd *cobra.Command) {
	var device_name string
	var cmd_catalog_list = &cobra.Command{
		Use:   SUB_CMD_CATALOG_LIST,
		Short: "list one or all of the devices defined in the catalog",
		Long:  `this command will list the specified or all of the existing block device definitions in the catalog.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if device_name == "" {
				this.catalog_list_all(this.catalog)
			} else {
				this.catalog_list_device(this.catalog, device_name)
			}
		},
	}
	cmd_catalog_list.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to display")

	root_cmd.AddCommand(cmd_catalog_list)
}

func (this *Lbd_lib) add_catalog_add(root_cmd *cobra.Command) {
	/* get block device definition and add it to the catalog.
	   this is really create-and-add so it should do the initialization of the backing
		 store as well, and fail to add if it is not unititialized */

	var device_name string
	var device_size uint64
	var storage_file string
	var directio bool
	var sync bool
	var alignment uint32
	/* this is optionally user supplied data, they don't know or care from the size + header
	they only care about the amount of data they want in one block. we will calculate the actual
	stree node size from the key length and value length */
	var stree_value_size uint32
	var calculated_stree_node_size uint32
	var additional_nodes_per_block uint32
	var mount bool
	var mountpoint string

	var cmd_catalog_add = &cobra.Command{
		Use:   SUB_CMD_CATALOG_ADD,
		Short: "add a catalog entry with this block device definition",
		Long: `this command will create a block device definition with the provided parameters from the command
			line and will add the block device definition to the catalog.`,
		Args: cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			calculated_stree_node_size = 0 // this is calculated, and must be zero

			var device = this.New_block_device(device_name, device_size, storage_file, directio, sync,
				alignment, stree_value_size, calculated_stree_node_size, additional_nodes_per_block,
				mount, mountpoint, false, false)
			this.catalog_add(this.catalog, device)
		},
	}
	cmd_catalog_add.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to create")
	cmd_catalog_add.Flags().StringVarP(&storage_file, TXT_STORAGE_FILE, "t", "", "path of file or block device for backing storage")
	cmd_catalog_add.Flags().Uint64VarP(&device_size, TXT_DEVICE_SIZE, "s", 0, "size in bytes of the block device, must be a multiple of "+tools.Inttostring(PHYSICAL_BLOCK_SIZE))
	cmd_catalog_add.Flags().BoolVarP(&directio, TXT_DIRECTIO, "i", false, "use O_DIRECT when reading and writing to backing storage")
	cmd_catalog_add.Flags().BoolVarP(&sync, TXT_SYNC, "n", false, "use O_SYNC when writing to backing storage")
	cmd_catalog_add.Flags().Uint32VarP(&alignment, TXT_ALIGNMENT, "a", 0, "align all backing storage writes to this byte alignment")
	cmd_catalog_add.Flags().Uint32VarP(&stree_value_size, TXT_NODE_VALUE_SIZE, "e", DEFAULT_NODE_VALUE_SIZE, "how many bytes to store in a data node, default: "+tools.Inttostring(DEFAULT_NODE_VALUE_SIZE))
	cmd_catalog_add.Flags().Uint32VarP(&additional_nodes_per_block, TXT_ADDITIONAL_NODES_PER_BLOCK, "p", 0, "how many additional nodes to add per block to make a single tree block")
	cmd_catalog_add.Flags().BoolVarP(&mount, TXT_MOUNT, "m", false, "try and mount filesystem after creating block device")
	cmd_catalog_add.Flags().StringVarP(&mountpoint, TXT_MOUNTPOINT, "r", "", "where to mount filesystem after creating block device") // required by user

	cmd_catalog_add.MarkFlagRequired(TXT_DEVICE_NAME)
	cmd_catalog_add.MarkFlagRequired(TXT_STORAGE_FILE)
	cmd_catalog_add.MarkFlagRequired(TXT_DEVICE_SIZE)

	root_cmd.AddCommand(cmd_catalog_add)
}

func (this *Lbd_lib) add_catalog_delete(root_cmd *cobra.Command) {
	var device_name string
	var i, am, sure bool
	var cmd_catalog_delete = &cobra.Command{
		Use:   SUB_CMD_CATALOG_DELETE,
		Short: "delete the specified catalog entry and its backing store",
		Long:  `this command will permanently destroy all data associated with the specified block device and remove it from the catalog.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			this.catalog_delete(this.catalog, device_name)
		},
	}
	cmd_catalog_delete.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to delete")
	cmd_catalog_delete.Flags().BoolVarP(&i, TXT_I, "i", false, "I")
	cmd_catalog_delete.Flags().BoolVarP(&am, TXT_AM, "a", false, "Am")
	cmd_catalog_delete.Flags().BoolVarP(&sure, TXT_SURE, "s", false, "Sure")

	cmd_catalog_delete.MarkFlagRequired(TXT_DEVICE_NAME)
	cmd_catalog_delete.MarkFlagRequired(TXT_I)
	cmd_catalog_delete.MarkFlagRequired(TXT_AM)
	cmd_catalog_delete.MarkFlagRequired(TXT_SURE)

	root_cmd.AddCommand(cmd_catalog_delete)
}

func (this *Lbd_lib) dragons_to_syslog(suffix string) {
	syslogger, err := syslog.New(syslog.LOG_INFO, this.application_name+"-"+suffix)
	if err != nil {
		log.Println("Unable to redirect logging to syslog: ", err)
	} else {
		log.SetOutput(syslogger)
	}
}

func (this *Lbd_lib) process_pipeline_command_line_params(data_pipeline *list.List,
	cmd *cobra.Command) tools.Ret {
	// run through each item in the pipeline and call process params on it.
	for item := this.data_pipeline.Front(); item != nil; item = item.Next() {
		var itemval = item.Value
		var pipline_element, ok = itemval.(zosbd2interfaces.Data_pipeline_element)
		// in go you must check for nil before casting to the list entry's type for some reason or it will panic
		if ok && pipline_element != nil {
			var ret = pipline_element.Process_parameters(cmd)
			if ret != nil {
				return ret
			}
		}
	}
	return nil
}

func (this *Lbd_lib) add_start_device_from_catalog(root_cmd *cobra.Command) {
	var device_name string
	var force bool
	var all bool
	var device_ramdisk bool
	var stree_ramdisk bool
	var dragons bool

	var cmd_catalog_start_device = &cobra.Command{
		Use:   SUB_CMD_CATALOG_START,
		Short: "create a block device with the definition specified by the device name in the catalog",
		Long: `this command will create a block device and create the backing storage (if a file is used) with the parameters specified by
 the device defined in the catalog for the specificed device name.`,
		Args: cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if device_ramdisk && stree_ramdisk {
				tools.Error(this.log, "you can only select one of device-ramdisk and stree-ramdisk")
				return
			}
			if len(device_name) > 0 && all {
				tools.Error(this.log, "you can only select one of device name and all")
				return
			}
			if len(device_name) == 0 && (all == false) {
				tools.Error(this.log, "you must select one of device name and all")
				return
			}

			/* give each item in the pipeline the opportunity to pick up it's command line params */
			var ret tools.Ret
			ret = this.process_pipeline_command_line_params(this.data_pipeline, cmd)
			if ret != nil {
				return
			}

			if dragons {
				// send all stdout to syslog if we're going to be running in the background
				this.dragons_to_syslog(device_name)
			}

			if all {
				ret = this.catalog_start_all(this.catalog, force, this.data_pipeline)
			} else {
				ret = this.catalog_start_device(this.catalog, device_name, force, this.data_pipeline, device_ramdisk, stree_ramdisk, dragons)
			}
			if ret != nil {
				return // it has already logged the error.
			}
		},
	}
	cmd_catalog_start_device.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device in the catalog to create")
	cmd_catalog_start_device.Flags().BoolVarP(&force, TXT_FORCE, "f", false, "force backing store to start even if not cleanly shut down")
	cmd_catalog_start_device.Flags().BoolVarP(&all, TXT_ALL, "a", false, "start all devices in catalog not excluded from starting")
	cmd_catalog_start_device.Flags().BoolVarP(&device_ramdisk, TXT_DEVICE_RAMDISK, "y", false, "for testing, use a ramdisk to back the block device")
	cmd_catalog_start_device.Flags().BoolVarP(&stree_ramdisk, TXT_STREE_RAMDISK, "j", false, "for testing, use a ramdisk to back the stree")
	cmd_catalog_start_device.Flags().BoolVarP(&dragons, TXT_DRAGONS, "H", false, "here be dragons")

	// cmd_catalog_start_device.MarkFlagRequired(TXT_DEVICE_NAME)

	root_cmd.AddCommand(cmd_catalog_start_device)
}

func (this *Lbd_lib) add_stop_device_from_catalog(root_cmd *cobra.Command) {
	// clean shutdown (will try and unmount)

	var device_name string
	var all bool
	var cmd_catalog_stop_device = &cobra.Command{
		Use:   SUB_CMD_CATALOG_STOP,
		Short: "cleanly shutdown a currently running block device specified by the device name",
		Long: `this command will attempt to unmount the block device (if the definition indicates it should have been mounted) and
			remove it from the active device list.`,
		Args: cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if len(device_name) > 0 && all {
				tools.Error(this.log, "you can only select one of device name and all")
				return
			}
			if len(device_name) == 0 && (all == false) {
				tools.Error(this.log, "you must select one of device name and all")
				return
			}
			var ret tools.Ret
			if all {
				ret = this.catalog_shutdown_all(this.catalog)
			} else {
				ret = this.catalog_shutdown_device(this.catalog, device_name)
			}
			if ret != nil {
				return // it has already logged the error.
			}
		},
	}
	cmd_catalog_stop_device.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device in the catalog to create")
	cmd_catalog_stop_device.Flags().BoolVarP(&all, TXT_ALL, "a", false, "stop all devices in catalog")

	root_cmd.AddCommand(cmd_catalog_stop_device)
}

/* set commands */

func (this *Lbd_lib) add_catalog_set_commands(cmd_catalog *cobra.Command) {
	var cmd_catalog_set = &cobra.Command{
		Use:   CMD_SET,
		Short: "set a configuration or catalog entry option",
		Long:  `this command will allow you to specify various configuration settings.`,
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			tools.Error(this.log, "please specify a subcommand for set")
		}}

	cmd_catalog.AddCommand(cmd_catalog_set)

	this.add_set_catalog_include_exclude(cmd_catalog_set)
}

func (this *Lbd_lib) add_set_catalog_include_exclude(cmd_catalog_set *cobra.Command) {

	var device_name string
	var cmd_catalog_set_include = &cobra.Command{
		Use:   CMD_INCLUDE,
		Short: "set a catalog entry to include on start all",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			this.set_catalog_entry_exclude_device(this.catalog, device_name, false)
		}}
	cmd_catalog_set_include.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to set include on")
	cmd_catalog_set_include.MarkFlagRequired(TXT_DEVICE_NAME)

	var cmd_catalog_set_exclude = &cobra.Command{
		Use:   CMD_EXCLUDE,
		Short: "set a catalog entry to exclude on start all",
		Args:  cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			this.set_catalog_entry_exclude_device(this.catalog, device_name, true)
		}}
	cmd_catalog_set_exclude.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to set exclude on")
	cmd_catalog_set_exclude.MarkFlagRequired(TXT_DEVICE_NAME)

	cmd_catalog_set.AddCommand(cmd_catalog_set_include)
	cmd_catalog_set.AddCommand(cmd_catalog_set_exclude)
}

// func (this *Lbd_lib) add_catalog_set_commands(cmd_catalog *cobra.Command) {
// 	var device_name string
// 	var include bool
// 	var exclude bool
// 	var cmd_catalog_set_exclude = &cobra.Command{
// 		Use:   CMD_EXCLUDE,
// 		Short: "set the config value for exclude value for a particular catalog entry",
// 		Long:  `this command will allow you to specify if you want to include or exclude a catalog entry when you run the catalog start all command `,
// 		Args:  cobra.MinimumNArgs(0),
// 		Run: func(cmd *cobra.Command, args []string) {

// 			// get the device name and set the value of on or off they specified and rewrite the catalog
// 			if (exclude && include) || (exclude == false && include == false) {
// 				tools.Error(this.log, "you must select one of include or exclude")
// 				return
// 			}

// 		}}
// 	cmd_catalog_set_exclude.Flags().StringVarP(&device_name, TXT_DEVICE_NAME, "d", "", "name of the block device to set exclude on")
// 	cmd_catalog_set_exclude.Flags().BoolVarP(&exclude, TXT_EXCLUDE, "e", false, "flag to exclude")
// 	cmd_catalog_set_exclude.Flags().BoolVarP(&include, TXT_INCLUDE, "i", false, "flag to include")

// 	cmd_catalog_set.AddCommand(cmd_catalog_set_exclude)
// }

func (this *Lbd_lib) cobra_commands_setup(root_cmd *cobra.Command) tools.Ret {

	cobra.OnInitialize(this.init_config_and_log)

	this.add_storage_status(root_cmd)
	this.add_device_status(root_cmd)
	//	this.add_create_block_device(root_cmd)
	this.add_destroy_block_device(root_cmd)
	this.add_destroy_all_block_devices(root_cmd)

	/* catalog */
	this.add_catalog_commands(root_cmd)

	/* diagnostics */

	this.add_diag_commands(root_cmd)

	return nil
}
