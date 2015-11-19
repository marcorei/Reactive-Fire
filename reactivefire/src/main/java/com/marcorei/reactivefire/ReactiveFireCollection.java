/* (c) 2015 Markus Riegel
 * license: MIT
 */
package com.marcorei.reactivefire;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.FirebaseException;
import com.firebase.client.Query;
import com.firebase.client.ValueEventListener;

import rx.Observable;
import rx.Subscriber;

/**
 * Collection that wraps Firebase listener in Observables.
 */
public class ReactiveFireCollection {

    /**
     * Get value each time data changes. Does not complete.
     * Uses {@link Query#addValueEventListener(ValueEventListener)}.
     * @param query Firebase query.
     * @param ItemClass Item class.
     * @param <T> Item class type.
     * @return Items via Observable.
     */
    public <T> Observable<T> getValue(final Query query, final Class<T> ItemClass) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> subscriber) {
                query.addValueEventListener(new ReactiveValueListener<>(subscriber, ItemClass, false));
            }
        });
    }

    /**
     * Get a value, don't wait for a sync to get data.
     * Might return old, cached data, but faster than {@link #getSingleValue(Query, Class)}.
     * Uses {@link Query#addValueEventListener(ValueEventListener)} but completes after the first event.
     * @param query Firebase query.
     * @param ItemClass Item class.
     * @param <T> Item class type.
     * @return Items via Observable.
     */
    public <T> Observable<T> getValueOnce(final Query query, final Class<T> ItemClass) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> subscriber) {
                query.addValueEventListener(new ReactiveValueListener<>(subscriber, ItemClass, true));
            }
        });
    }

    /**
     * Get a value once with synced data.
     * Uses {@link Query#addListenerForSingleValueEvent(ValueEventListener)}.
     * @param query Firebase query.
     * @param ItemClass Item class.
     * @param <T> Item class type.
     * @return Items via Observable.
     */
    public <T> Observable<T> getSingleValue(final Query query, final Class<T> ItemClass) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> subscriber) {
                query.addListenerForSingleValueEvent(new ReactiveValueListener<>(subscriber, ItemClass, true));
            }
        });
    }

    /**
     * Uses {@link Firebase#setValue(Object, Firebase.CompletionListener)}.
     * @param reference Target location.
     * @param value Value to set.
     * @param <T> Value type.
     * @return Void Observable.
     */
    public <T> Observable<Void> setValue(final Firebase reference, final T value) {
        return Observable.create(new Observable.OnSubscribe<Void>() {
            @Override
            public void call(final Subscriber<? super Void> subscriber) {
                reference.setValue(value, new ReactiveCompletionListener(subscriber));
            }
        });
    }

    /**
     * Uses {@link Firebase#push()}, then {@link Firebase#setValue(Object, Firebase.CompletionListener)}.
     * @param reference Target location.
     * @param value Value to set.
     * @param <T> Value type.
     * @return Void Observable.
     */
    public <T> Observable<Void> pushValue(Firebase reference, T value) {
       return setValue(reference.push(), value);
    }

    /**
     * Uses {@link Firebase#removeValue()}.
     * @param reference Target location.
     * @return Void Observable.
     */
    public Observable <Void> removeValue(final Firebase reference) {
        return Observable.create(new Observable.OnSubscribe<Void>() {
            @Override
            public void call(Subscriber<? super Void> subscriber) {
                reference.removeValue(new ReactiveCompletionListener(subscriber));
            }
        });
    }

    private class ReactiveValueListener<T> implements ValueEventListener {
        private Subscriber<? super T> subscriber;
        private Class<T> ItemClass;
        private boolean once;

        public ReactiveValueListener(Subscriber<? super T> subscriber, Class<T> ItemClass, boolean once) {
            this.subscriber = subscriber;
            this.ItemClass = ItemClass;
            this.once = once;
        }

        @Override
        public void onDataChange(DataSnapshot dataSnapshot) {
            T value;
            try {
                value = dataSnapshot.getValue(ItemClass);
            }
            catch(FirebaseException firebaseException) {
                value = null;
            }
            subscriber.onNext(value);
            if(once) {
                subscriber.onCompleted();
            }
        }

        @Override
        public void onCancelled(FirebaseError firebaseError) {
            subscriber.onError(firebaseError.toException());
        }
    }

    private class ReactiveCompletionListener implements Firebase.CompletionListener {
        private Subscriber<? super Void> subscriber;

        public ReactiveCompletionListener(Subscriber<? super Void> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onComplete(FirebaseError firebaseError, Firebase firebase) {
            if(firebaseError != null) {
                subscriber.onError(firebaseError.toException());
            }
            else {
                subscriber.onCompleted();
            }
        }
    }
}
