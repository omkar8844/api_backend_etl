"""
Controller script that runs four scripts in sequence using the schedule library.
Runs: transfrom_b.py → trans2.py → items_silver.py → gold.py, then loops back.
"""

import schedule
import time
import subprocess
import sys
import os


def run_script(script_name):
    """Run a Python script and return True if successful, False otherwise."""
    try:
        print(f"\n{'='*60}")
        print(f"Starting: {script_name}")
        print(f"{'='*60}")
        
        result = subprocess.run(
            [sys.executable, script_name],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            check=True,
            capture_output=False,
            text=True
        )
        
        print(f"\n✓ Completed: {script_name}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error running {script_name}: {e}")
        return False
    except Exception as e:
        print(f"\n✗ Unexpected error running {script_name}: {e}")
        return False


def run_script_cycle():
    """Run all four scripts in sequence, then immediately schedule next cycle."""
    scripts = [
        'transfrom_b.py',
        'trans2.py',
        'items_silver.py',
        'gold.py'
    ]
    
    print(f"\n{'#'*60}")
    print(f"Starting script cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}")
    
    for script in scripts:
        if not run_script(script):
            print(f"\nWarning: {script} failed, but continuing with next script...")
    
    print(f"\n{'#'*60}")
    print(f"Completed script cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*60}\n")
    
    # Schedule the next cycle to run immediately (back-to-back)
    # Clear existing jobs and reschedule for continuous execution
    schedule.clear()
    schedule.every(1).second.do(run_script_cycle)


def main():
    """Main function to start the scheduler."""
    print("="*60)
    print("Script Scheduler Started")
    print("="*60)
    print("Running scripts in sequence:")
    print("  1. transfrom_b.py")
    print("  2. trans2.py")
    print("  3. items_silver.py")
    print("  4. gold.py")
    print("Then looping back to transfrom_b.py...")
    print("="*60)
    
    # Start the first cycle immediately, then use scheduler for subsequent cycles
    run_script_cycle()
    
    # Run the scheduler continuously for subsequent cycles
    try:
        while True:
            schedule.run_pending()
            time.sleep(0.1)  # Small sleep to prevent CPU spinning
    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
