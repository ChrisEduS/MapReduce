class MyThread extends Thread {
    private int value;
    private String[] results;

    public MyThread(int value, String[] results) {
        this.value = value;
        this.results = results;
    }

    public void run() {
        // Simulate some computation
        try {
            Thread.sleep(100);  // Sleep for 100 milliseconds to simulate computation time
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Store the result in the array
        results[value - 1] = Thread.currentThread().getId() + " Value " + value;
    }
}
