/* (c) 2015 Markus Riegel
 * license: MIT
 */
package com.marcorei.reactivefiredemo.ui;

import android.app.Application;

import com.firebase.client.Firebase;

/**
 * Reactive Fire Demo Application
 */
public class ReactiveFireDemoApp extends Application{
    @Override
    public void onCreate() {
        super.onCreate();
        Firebase.setAndroidContext(this);
    }
}
