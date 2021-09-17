package com.github.zhangchunsheng.flink.pomegranate;

import com.github.zhangchunsheng.flink.model.EquipmentWorkTime;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkWorkTimeToMySQL extends RichSinkFunction<EquipmentWorkTime> {
    private static final Logger logger = LoggerFactory.getLogger(SinkWorkTimeToMySQL.class);

    PreparedStatement ps;
    BasicDataSource dataSource;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into c_equipment_work_time_t(package_date, start_package_time, status, equipment_number, " +
                "status_duration, duration_minute, ip, end_package_time, package_no, work_time, " +
                "standby_time, warning_time, piece_cnt) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(EquipmentWorkTime value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //遍历数据集合
        EquipmentWorkTime equipmentWorkTime = value;
        //for (Student student : value) {
        ps.setInt(1, equipmentWorkTime.getPackageDate());
        ps.setLong(2, equipmentWorkTime.getStartPackageTime());
        ps.setInt(3, equipmentWorkTime.getStatus());
        ps.setString(4, equipmentWorkTime.getEquipmentNumber());
        ps.setInt(5, equipmentWorkTime.getStatusDuration());
        ps.setDouble(6, equipmentWorkTime.getDurationMinute());
        ps.setString(7, equipmentWorkTime.getIp());
        ps.setLong(8, equipmentWorkTime.getEndPackageTime());
        ps.setInt(9, equipmentWorkTime.getPackageNo());
        ps.setLong(10, equipmentWorkTime.getWorkTime());
        ps.setLong(11, equipmentWorkTime.getStandbyTime());
        ps.setLong(12, equipmentWorkTime.getWarningTime());
        ps.setInt(13, equipmentWorkTime.getPieceCnt());
        ps.addBatch();
        //}
        int[] count = ps.executeBatch();//批量后执行
        logger.info("成功了插入了 {} 行数据", count.length);
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://192.168.0.186:9030/camtg");
        dataSource.setUsername("root");
        dataSource.setPassword("camtg");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            logger.info("创建连接池：{}", con);
        } catch (Exception e) {
            logger.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
