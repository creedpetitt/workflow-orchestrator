package com.creedpetitt.exampleworker;

import com.creedpetitt.workersdk.Worker;

public class ExampleWorkerApp {

    public static void main(String[] args) {
        Worker worker = new Worker();

        // Register handlers for each action
        worker.register("step1", payload -> {
            System.out.println("Step1 processing: " + payload);
            // Simulate task
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "step1-result: processed " + payload;
        });

        worker.register("step2", payload -> {
            System.out.println("Step2 processing: " + payload);
            // Simulate task
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "step2-result: finished with " + payload;
        });

        // Start the worker
        worker.start("jdbc:postgresql://postgres:5432/wf_engine", "admin", "admin");
    }
}
