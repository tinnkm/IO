package com.tinnkm.kafka.common.utils;

public class Timer {
    private final Time time;
    private long startMs;
    private long currentTimeMs;
    private long deadlineMs;
    private long timeoutMs;

    Timer(Time time, long timeoutMs) {
        this.time = time;
        update();
        reset(timeoutMs);
    }

    /**
     * Check timer expiration. Like {@link #remainingMs()}, this depends on the current cached
     * time in milliseconds, which is only updated through one of the {@link #update()} methods
     * or with {@link #sleep(long)};
     *
     * @return true if the timer has expired, false otherwise
     */
    public boolean isExpired() {
        return currentTimeMs >= deadlineMs;
    }

    /**
     * Check whether the timer has not yet expired.
     * @return true if there is still time remaining before expiration
     */
    public boolean notExpired() {
        return !isExpired();
    }

    /**
     * Reset the timer to the specific timeout. This will use the underlying {@link #Timer(Time, long)}
     * implementation to update the current cached time in milliseconds and it will set a new timer
     * deadline.
     *
     * @param timeoutMs The new timeout in milliseconds
     */
    public void updateAndReset(long timeoutMs) {
        update();
        reset(timeoutMs);
    }

    /**
     * Reset the timer using a new timeout. Note that this does not update the cached current time
     * in milliseconds, so it typically must be accompanied with a separate call to {@link #update()}.
     * Typically, you can just use {@link #updateAndReset(long)}.
     *
     * @param timeoutMs The new timeout in milliseconds
     */
    public void reset(long timeoutMs) {
        if (timeoutMs < 0)
            throw new IllegalArgumentException("Invalid negative timeout " + timeoutMs);

        this.timeoutMs = timeoutMs;
        this.startMs = this.currentTimeMs;

        if (currentTimeMs > Long.MAX_VALUE - timeoutMs)
            this.deadlineMs = Long.MAX_VALUE;
        else
            this.deadlineMs = currentTimeMs + timeoutMs;
    }

    /**
     * Reset the timer's deadline directly.
     *
     * @param deadlineMs The new deadline in milliseconds
     */
    public void resetDeadline(long deadlineMs) {
        if (deadlineMs < 0)
            throw new IllegalArgumentException("Invalid negative deadline " + deadlineMs);

        this.timeoutMs = Math.max(0, deadlineMs - this.currentTimeMs);
        this.startMs = this.currentTimeMs;
        this.deadlineMs = deadlineMs;
    }

    /**
     * Use the underlying {@link Time} implementation to update the current cached time. If
     * the underlying time returns a value which is smaller than the current cached time,
     * the update will be ignored.
     */
    public void update() {
        update(time.milliseconds());
    }

    /**
     * Update the cached current time to a specific value. In some contexts, the caller may already
     * have an accurate time, so this avoids unnecessary calls to system time.
     *
     * Note that if the updated current time is smaller than the cached time, then the update
     * is ignored.
     *
     * @param currentTimeMs The current time in milliseconds to cache
     */
    public void update(long currentTimeMs) {
        this.currentTimeMs = Math.max(currentTimeMs, this.currentTimeMs);
    }

    /**
     * Get the remaining time in milliseconds until the timer expires. Like {@link #currentTimeMs},
     * this depends on the cached current time, so the returned value will not change until the timer
     * has been updated using one of the {@link #update()} methods or {@link #sleep(long)}.
     *
     * @return The cached remaining time in milliseconds until timer expiration
     */
    public long remainingMs() {
        return Math.max(0, deadlineMs - currentTimeMs);
    }

    /**
     * Get the current time in milliseconds. This will return the same cached value until the timer
     * has been updated using one of the {@link #update()} methods or {@link #sleep(long)} is used.
     *
     * Note that the value returned is guaranteed to increase monotonically even if the underlying
     * {@link Time} implementation goes backwards. Effectively, the timer will just wait for the
     * time to catch up.
     *
     * @return The current cached time in milliseconds
     */
    public long currentTimeMs() {
        return currentTimeMs;
    }

    /**
     * Get the amount of time that has elapsed since the timer began. If the timer was reset, this
     * will be the amount of time since the last reset.
     *
     * @return The elapsed time since construction or the last reset
     */
    public long elapsedMs() {
        return currentTimeMs - startMs;
    }

    /**
     * Get the current timeout value specified through {@link #reset(long)} or {@link #resetDeadline(long)}.
     * This value is constant until altered by one of these API calls.
     *
     * @return The timeout in milliseconds
     */
    public long timeoutMs() {
        return timeoutMs;
    }

    /**
     * Sleep for the requested duration and update the timer. Return when either the duration has
     * elapsed or the timer has expired.
     *
     * @param durationMs The duration in milliseconds to sleep
     */
    public void sleep(long durationMs) {
        long sleepDurationMs = Math.min(durationMs, remainingMs());
        time.sleep(sleepDurationMs);
        update();
    }
}
