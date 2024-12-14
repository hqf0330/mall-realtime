package org.mason77.util;

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {

    public static Connection getConnection(String className, String url, String username, String password) throws ClassNotFoundException, SQLException {
        Class.forName(className);
        return DriverManager.getConnection(url, username, password);
    }

    public static <T> List<T> queryList(Connection conn, String querySql, Class<T> tClass, boolean... isUnderlineToCamel) throws Exception {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        List<T> result = new ArrayList<>();
        // 1. 预编译
        PreparedStatement preparedStatement = conn.prepareStatement(querySql);
        // 2. 执行查询, 获得结果集
        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        // 3. 解析结果集, 把数据封装到一个 List 集合中
        while (resultSet.next()) {
            // 变量到一行数据, 把这个行数据封装到一个 T 类型的对象中
            T t = tClass.newInstance(); // 使用反射创建一个 T 类型的对象
            // 遍历这一行的每一列数据
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                // 获取列名
                // 获取列值
                String name = metaData.getColumnLabel(i);
                Object value = resultSet.getObject(name);

                if (defaultIsUToC) { // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                    name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
                }

                // t.name=value
                BeanUtils.setProperty(t, name, value);
            }
            result.add(t);
        }
        return result;
    }

    public static void closeConnection(Connection conn) throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
