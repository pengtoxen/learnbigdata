package com.peng.web.controller;

import com.alibaba.fastjson.JSON;
import com.peng.common_behavior.ErrorReportLogs;
import com.peng.common_behavior.PageVisitReportLogs;
import com.peng.common_behavior.StartupReportLogs;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;

/**
 * 接收日志的服务器
 */
@Controller()
@RequestMapping("/logs")
public class LogCollector {

    private static final Logger logger = Logger.getLogger(LogCollector.class);

    @RequestMapping(value = "/startupLogs", method = RequestMethod.POST)
    @ResponseBody
    public StartupReportLogs startupCollect(@RequestBody StartupReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        // 写入日志目录
        logger.info(LogString);

        return e;
    }

    @RequestMapping(value = "/pageLogs", method = RequestMethod.POST)
    @ResponseBody
    public PageVisitReportLogs pageCollect(@RequestBody PageVisitReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        // 写入日志目录
        logger.info(LogString);

        return e;
    }

    @RequestMapping(value = "/errorLogs", method = RequestMethod.POST)
    @ResponseBody
    public ErrorReportLogs errorCollect(@RequestBody ErrorReportLogs e, HttpServletRequest req) {

        String LogString = JSON.toJSONString(e);

        // 写入日志目录
        logger.info(LogString);

        return e;
    }
}