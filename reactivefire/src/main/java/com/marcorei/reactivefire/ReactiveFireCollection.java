/* (c) 2015 Markus Riegel
 * license: MIT
 */
package com.marcorei.reactivefire;

import 	com.google.firebase.database.ChildEventListener;
import 	com.google.firebase.database.DataSnapshot;
import 	com.google.firebase.database.DatabaseReference;
import 	com.google.firebase.database.DatabaseError;
import 	com.google.firebase.database.DatabaseException;
import 	com.google.firebase.database.Query;
import 	com.google.firebase.database.ValueEventListener;

import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

/**
 * Collection that wraps Firebase listener in Observables.
 */
public class ReactiveFireCollection {

    /**
     * Get a snapshot of each child in or added to the provided Query.
     * @param query Firebase query.
     * @return DataSnapshots via Observable.
     */
    public Observable<DataSnapshot> getChildren(final Query query) {
        return Observable.create(new Observable.OnSubscribe<DataSnapshot>() {
            @Override
            public void call(final Subscriber<? super DataSnapshot> subscriber) {
                final ReactiveChildListener listener = new ReactiveChildListener(subscriber);
                query.addChildEventListener(listener);
                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        query.removeEventListener(listener);
                    }
                }));
            }
        });
    }

    /**
     * Get value each time data changes. Does not complete.
     * Uses {@link Query#addValueEventListener(ValueEventListener)}.
     * @param query Firebase query.
     * @return DataSnapshots via Observable.
     */
    public Observable<DataSnapshot> getValue(final Query query) {
        return Observable.create(new Observable.OnSubscribe<DataSnapshot>() {
            @Override
            public void call(Subscriber<? super DataSnapshot> subscriber) {
                final ReactiveValueListener listener = new ReactiveValueListener(subscriber, false);
                query.addValueEventListener(listener);
                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        query.removeEventListener(listener);
                    }
                }));
            }
        });
    }

    /**
     * Get a value, don't wait for a sync to get data.
     * Might return old, cached data, but faster than {@link #getSingleValue(Query)}.
     * Uses {@link Query#addValueEventListener(ValueEventListener)} but completes after the first event.
     * @param query Firebase query.
     * @return DataSnapshots via Observable.
     */
    public Observable<DataSnapshot> getValueOnce(final Query query) {
        return Observable.create(new Observable.OnSubscribe<DataSnapshot>() {
            @Override
            public void call(Subscriber<? super DataSnapshot> subscriber) {
                final ReactiveValueListener listener = new ReactiveValueListener(subscriber, true);
                query.addValueEventListener(listener);
                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        query.removeEventListener(listener);
                    }
                }));
            }
        });
    }

    /**
     * Get a value once with synced data.
     * Uses {@link Query#addListenerForSingleValueEvent(ValueEventListener)}.
     * @param query Firebase query.
     * @return DataSnapshots via Observable.
     */
    public Observable<DataSnapshot> getSingleValue(final Query query) {
        return Observable.create(new Observable.OnSubscribe<DataSnapshot>() {
            @Override
            public void call(Subscriber<? super DataSnapshot> subscriber) {
                final ReactiveValueListener listener = new ReactiveValueListener(subscriber, true);
                query.addListenerForSingleValueEvent(listener);
                subscriber.add(Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        query.removeEventListener(listener);
                    }
                }));
            }
        });
    }

    /**
     * Uses {@link DatabaseReference#setValue(Object, DatabaseReference.CompletionListener)}.
     * @param reference Target location.
     * @param value Value to set.
     * @param <T> Value type.
     * @return Void Observable.
     */
    public <T> Observable<DatabaseReference> setValue(final DatabaseReference reference, final T value) {
        return Observable.create(new Observable.OnSubscribe<DatabaseReference>() {
            @Override
            public void call(final Subscriber<? super DatabaseReference> subscriber) {
                reference.setValue(value, new ReactiveCompletionListener(subscriber));
                // Can't remove complete listeners.
            }
        });
    }

    /**
     * Uses {@link DatabaseReference#push()}, then {@link DatabaseReference#setValue(Object, DatabaseReference.CompletionListener)}.
     * @param reference Target location.
     * @param value Value to set.
     * @param <T> Value type.
     * @return Void Observable.
     */
    public <T> Observable<DatabaseReference> pushValue(DatabaseReference reference, T value) {
       return setValue(reference.push(), value);
    }

    /**
     * Uses {@link DatabaseReference#removeValue()}.
     * @param reference Target location.
     * @return Void Observable.
     */
    public Observable <DatabaseReference> removeValue(final DatabaseReference reference) {
        return Observable.create(new Observable.OnSubscribe<DatabaseReference>() {
            @Override
            public void call(Subscriber<? super DatabaseReference> subscriber) {
                reference.removeValue(new ReactiveCompletionListener(subscriber));
                // Can't remove complete listeners.
            }
        });
    }

    /**
     * Marshal snapshots to typed objects.
     * @param ItemClass Target class.
     * @param <T> target type.
     * @return Function e.g. for use with {@link Observable#map(Func1)}.
     */
    public <T> Func1<DataSnapshot, T> marshalSnapshot(final Class<T> ItemClass) {
        return new Func1<DataSnapshot, T>() {
            @Override
            public T call(DataSnapshot dataSnapshot) {
                try {
                    return dataSnapshot.getValue(ItemClass);
                }
                catch(DatabaseException exception){
                    return null;
                }
            }
        };
    }

    private class ReactiveChildListener implements ChildEventListener {
        private Subscriber<? super DataSnapshot> subscriber;

        public ReactiveChildListener(Subscriber<? super DataSnapshot> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onChildAdded(DataSnapshot dataSnapshot, String s) {
            subscriber.onNext(dataSnapshot);
        }

        @Override
        public void onChildChanged(DataSnapshot dataSnapshot, String s) {}

        @Override
        public void onChildRemoved(DataSnapshot dataSnapshot) {}

        @Override
        public void onChildMoved(DataSnapshot dataSnapshot, String s) {}

        @Override
        public void onCancelled(DatabaseError firebaseError) {
            subscriber.onError(firebaseError.toException());
        }
    }

    private class ReactiveValueListener implements ValueEventListener {
        private Subscriber<? super DataSnapshot> subscriber;
        private boolean once;

        public ReactiveValueListener(Subscriber<? super DataSnapshot> subscriber, boolean once) {
            this.subscriber = subscriber;
            this.once = once;
        }

        @Override
        public void onDataChange(DataSnapshot dataSnapshot) {
            subscriber.onNext(dataSnapshot);
            if(once) {
                subscriber.onCompleted();
            }
        }

        @Override
        public void onCancelled(DatabaseError firebaseError) {
            subscriber.onError(firebaseError.toException());
        }
    }

    private class ReactiveCompletionListener implements DatabaseReference.CompletionListener {
        private Subscriber<? super DatabaseReference> subscriber;

        public ReactiveCompletionListener(Subscriber<? super DatabaseReference> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onComplete(DatabaseError firebaseError, DatabaseReference firebase) {
            if(firebaseError != null) {
                subscriber.onError(firebaseError.toException());
            }
            else {
                subscriber.onNext(firebase);
                subscriber.onCompleted();
            }
        }
    }
}
