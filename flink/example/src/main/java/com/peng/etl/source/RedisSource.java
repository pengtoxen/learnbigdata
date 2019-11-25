package com.peng.etl.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.HashMap;
import java.util.Map;

/**
 * redis客户端执行下列命令,插入数据
 * hset areas AREA_US US
 * hset areas AREA_CT TW,HK
 * hset areas AREA_AR PK,KW,SA
 * hset areas AREA_IN IN
 * <p>
 * 自定义source,从redis中读取数据
 *
 * @author Administrator
 */
public class RedisSource implements SourceFunction<HashMap<String, String>> {

    private Logger logger = LoggerFactory.getLogger(RedisSource.class);

    private Jedis jedis = null;

    private boolean isRunning = false;

    @Override
    public void run(SourceContext<HashMap<String, String>> sourceContext) throws Exception {

        //初始化redis客户端,只执行一次
        this.jedis = new Jedis("localhost", 6379);

        //初始化map,接收redis中读出的数据
        HashMap<String, String> map = new HashMap<>();

        //isRunning=true,每隔60秒,从redis中读一次数据
        while (isRunning) {
            try {
                //读取之前,把上次的读到的数据清空掉
                map.clear();
                //获取key为areas的数据
                Map<String, String> areas = jedis.hgetAll("areas");
                //遍历map
                for (Map.Entry<String, String> entry : areas.entrySet()) {
                    String area = entry.getKey();
                    String value = entry.getValue();
                    //value是多个PK,KW,SA,拆开
                    //(AREA_AR => PK,KW,SA)
                    String[] fields = value.split(",");
                    for (String country : fields) {
                        //以country简写为key,area为value
                        //(AREA_AR => PK)
                        //(AREA_AR => KW)
                        //(AREA_AR => SA)
                        map.put(country, area);
                    }
                }
                //map>0已经有数据,输出到下游
                if (map.size() > 0) {
                    sourceContext.collect(map);
                }
                //间隔60秒,从redis中重新拿数据
                Thread.sleep(60000);
            } catch (JedisConnectionException e) {
                logger.error("redis连接异常", e.getCause());
                this.jedis = new Jedis("localhost", 6379);
            } catch (Exception e) {
                logger.error("数据源异常", e.getCause());
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
        if (this.jedis != null) {
            this.jedis.close();
        }
    }
}
