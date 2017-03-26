package exprdb.store;

import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeStamp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Clock {

  private NTPServerTimeProvider timeServer;
  private long logicalTime;
  private long logicalCounter;

  public Clock() {
    this(new TimeStamp(new Date(0)).ntpValue(), 0);
  }

  public Clock(long timestamp) {
    this(timestamp >> 16, timestamp << 48 >> 48);
  }

  public Clock(long logicalTime, long logicalCounter) {
    this.timeServer = null;
    try {
      this.timeServer = new NTPServerTimeProvider();
      this.timeServer.update();
    } catch (UnknownHostException e) {}
    this.logicalTime = logicalTime;
    this.logicalCounter = logicalCounter;
  }

  public long update() {
    long physicalTime = timeServer.getPhysicalTime();
    if (compare(logicalTime, physicalTime) < 0) {
      logicalTime = physicalTime;
      logicalCounter = 0;
    } else {
      logicalCounter++;
    }
    return timestamp();
  }

  public long update(Clock ts) {
    return update(ts.logicalTime, ts.logicalCounter);
  }

  public long update(long eventLogicalTime, long eventLogicalCounter) {
    long physicalTime = timeServer.getPhysicalTime();
    if (compare(physicalTime, eventLogicalTime) > 0 &&
        compare(physicalTime, logicalTime) > 0) {
      logicalTime = physicalTime;
      logicalCounter = 0;
    } else if (compare(eventLogicalTime, logicalTime) > 0) {
      logicalTime = eventLogicalTime;
      logicalCounter++;
    } else if (compare(logicalTime, eventLogicalTime) > 0) {
      logicalCounter++;
    } else {
      if (eventLogicalCounter > logicalCounter) {
        logicalCounter = eventLogicalCounter;
      }
      logicalCounter++;
    }
    return timestamp();
  }

  public long timestamp() {
    return (logicalTime >> 16 << 16) | (logicalCounter << 48 >> 48);
  }


  public String toString() {
    String logical = new TimeStamp(logicalTime).toUTCString();
    String timeStamp = new TimeStamp(timestamp()).toUTCString();
    return "<Clock logical=" + logical + "@" + logicalCounter + " NTP=" +  timeStamp + "/" + timestamp
      () + ">";
  }

  public static int compare(long time1, long time2) {
    TimeStamp t1 = new TimeStamp(time1);
    TimeStamp t2 = new TimeStamp(time2);
    return compare(t1, t2);
  }

  public static int compare(TimeStamp t1, TimeStamp t2) {
    if (t1.getSeconds() == t2.getSeconds() && t1.getFraction() == t2.getFraction()) {
      return 0;
    }
    if (t1.getSeconds() == t2.getSeconds()) {
      return t1.getFraction() < t2.getFraction() ? -1 : 1;
    }
    return t1.getSeconds() < t2.getSeconds() ? -1 : 1;
  }

  public int compareTo(Clock o) {
    int ntpComparison = compare(logicalTime, o.logicalTime);
    if (ntpComparison == 0) {
      return new Long(logicalCounter).compareTo(o.logicalCounter);
    } else {
      return ntpComparison;
    }
  }

  public static class NTPServerTimeProvider {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private static final String[] DEFAULT_NTP_SERVERS = {"localhost", "0.pool.ntp.org", "1.pool.ntp.org", "2.pool.ntp.org", "3.pool.ntp.org"};

    private final NTPUDPClient client;
    private List<InetAddress> servers;

    private TimeStamp timestamp;
    private long nano;

    private long delay = 30;
    private TimeUnit delayUnits = TimeUnit.MINUTES;


    public NTPServerTimeProvider() throws UnknownHostException {
      this(DEFAULT_NTP_SERVERS);
    }

    public NTPServerTimeProvider(String[] ntpServers) throws UnknownHostException {
      client = new NTPUDPClient();
      setServers(ntpServers);
      if (servers.isEmpty()) {
        throw new UnknownHostException(ntpServers[0]);
      }
    }

    protected void setServers(String[] ntpServers) {
      servers = Arrays.asList(ntpServers).stream().map(server -> {
          try {
            return InetAddress.getByName(server);
          } catch (UnknownHostException e) {
            return null;
          }
        }).filter(address -> address != null).collect(Collectors.toList());
    }

    public synchronized void update() {
      InetAddress server = servers.remove(0);
      try {
        timestamp = client.getTime(server).getMessage().getTransmitTimeStamp();
        nano = System.nanoTime();
        servers.add(0, server);
      } catch (IOException e) {
        servers.add(server);
      }
    }

    TimeStamp getTimestamp() {
      TimeStamp ts = new TimeStamp(timestamp.ntpValue());
      long fraction = ts.getFraction();
      long seconds = ts.getSeconds();
      long nanoTime = System.nanoTime();
      long l = (nanoTime - nano) / 1_000_000_000;
      double v = (nanoTime - nano) / 1_000_000_000.0 - l;
      long i = (long) (v * 1_000_000_000);
      long fraction_ = fraction + i;
      if (fraction_ >= 1_000_000_000) {
        fraction_ -= 1_000_000_000;
        l++;
      }
      return new TimeStamp((seconds + l) << 32 | fraction_);
    }

    public long getPhysicalTime() {
      return getTimestamp().ntpValue();
    }

  }

  public static void main(String[] args) throws Exception {
    Clock clock = new Clock();
    System.out.println("ts:"+clock);
    clock.update();
    System.out.println("ts:"+clock);
    clock.update();
    System.out.println("ts:"+clock);
  }

}
