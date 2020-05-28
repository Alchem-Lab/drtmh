#include <linux/module.h>    // included for all kernel modules
#include <linux/kernel.h>    // included for KERN_INFO
#include <linux/init.h>      // included for __init and __exit macros
#include <linux/cpumask.h>
#include <asm/msr.h>

MODULE_LICENSE("GPL");
MODULE_AUTHOR("RTM");
MODULE_DESCRIPTION("A simple module for re-enabling RTM");

static int __init actrtm_init(void)
{
	int err = 0;
	u32 reg = 0x105;
	int cpu_id;

	for (cpu_id = 0; cpu_id < num_online_cpus(); cpu_id++) {
		err = wrmsr_safe_on_cpu(cpu_id, reg, 0x1, 0x0);
		if (err)
			printk(KERN_INFO "wrmsr_safe_on_cpu on cpu %d failed.\n", cpu_id);
		else
			printk(KERN_INFO "wrmsr_safe_on_cpu on cpu %d succeed.\n", cpu_id);
	}
	return 0;    // Non-zero return means that the module couldn't be loaded.
}

static void __exit actrtm_cleanup(void)
{
	printk(KERN_INFO "Cleaning up actrtm module.\n");
}

module_init(actrtm_init);
module_exit(actrtm_cleanup);
