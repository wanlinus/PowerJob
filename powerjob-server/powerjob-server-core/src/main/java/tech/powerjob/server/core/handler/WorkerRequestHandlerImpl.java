package tech.powerjob.server.core.handler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import tech.powerjob.common.RemoteConstant;
import tech.powerjob.common.enums.InstanceStatus;
import tech.powerjob.common.request.TaskTrackerReportInstanceStatusReq;
import tech.powerjob.common.request.WorkerHeartbeat;
import tech.powerjob.common.request.WorkerLogReportReq;
import tech.powerjob.common.response.AskResponse;
import tech.powerjob.remote.framework.actor.Actor;
import tech.powerjob.server.core.instance.InstanceLogService;
import tech.powerjob.server.core.instance.InstanceManager;
import tech.powerjob.server.core.service.MD5Utils;
import tech.powerjob.server.core.util.HttpClientUtil;
import tech.powerjob.server.core.workflow.WorkflowInstanceManager;
import tech.powerjob.server.monitor.MonitorService;
import tech.powerjob.server.monitor.events.w2s.TtReportInstanceStatusEvent;
import tech.powerjob.server.monitor.events.w2s.WorkerHeartbeatEvent;
import tech.powerjob.server.monitor.events.w2s.WorkerLogReportEvent;
import tech.powerjob.server.persistence.remote.model.AppInfoDO;
import tech.powerjob.server.persistence.remote.model.JobInfoDO;
import tech.powerjob.server.persistence.remote.repository.AppInfoRepository;
import tech.powerjob.server.persistence.remote.repository.ContainerInfoRepository;
import tech.powerjob.server.persistence.remote.repository.JobInfoRepository;
import tech.powerjob.server.remote.worker.WorkerClusterManagerService;
import tech.powerjob.server.remote.worker.WorkerClusterQueryService;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * receive and process worker's request
 *
 * @author tjq
 * @since 2022/9/11
 */
@Slf4j
@Component
@Actor(path = RemoteConstant.S4W_PATH)
public class WorkerRequestHandlerImpl extends AbWorkerRequestHandler {

    private final InstanceManager instanceManager;

    private final WorkflowInstanceManager workflowInstanceManager;

    private final InstanceLogService instanceLogService;

    public WorkerRequestHandlerImpl(InstanceManager instanceManager, WorkflowInstanceManager workflowInstanceManager, InstanceLogService instanceLogService,
                                    MonitorService monitorService, Environment environment, ContainerInfoRepository containerInfoRepository, WorkerClusterQueryService workerClusterQueryService) {
        super(monitorService, environment, containerInfoRepository, workerClusterQueryService);
        this.instanceManager = instanceManager;
        this.workflowInstanceManager = workflowInstanceManager;
        this.instanceLogService = instanceLogService;
    }

    @Override
    protected void processWorkerHeartbeat0(WorkerHeartbeat heartbeat, WorkerHeartbeatEvent event) {
        WorkerClusterManagerService.updateStatus(heartbeat);
    }

    @Override
    protected AskResponse processTaskTrackerReportInstanceStatus0(TaskTrackerReportInstanceStatusReq req, TtReportInstanceStatusEvent event) throws Exception {
        // 2021/02/05 如果是工作流中的实例先尝试更新上下文信息，再更新实例状态，这里一定不会有异常
        if (req.getWfInstanceId() != null && !CollectionUtils.isEmpty(req.getAppendedWfContext())) {
            // 更新工作流上下文信息
            workflowInstanceManager.updateWorkflowContext(req.getWfInstanceId(),req.getAppendedWfContext());
        }

        instanceManager.updateStatus(req);

        // 结束状态（成功/失败）需要回复消息
        if (InstanceStatus.FINISHED_STATUS.contains(req.getInstanceStatus())) {
            return AskResponse.succeed(null);
        }

        return null;
    }

    @Override
    protected void processWorkerLogReport0(WorkerLogReportReq req, WorkerLogReportEvent event) {
        // 这个效率应该不会拉垮吧...也就是一些判断 + Map#get 吧...
        instanceLogService.submitLogs(req.getWorkerAddress(), req.getInstanceLogContents());
    }

    @Resource
    private AppInfoRepository appInfoRepository;

    @Resource
    private JobInfoRepository jobInfoRepository;


    @Value("${spring.datasource.core.jdbc-url}")
    private String flywayUrl;
    @Value("${spring.datasource.core.username}")
    private String username;
    @Value("${spring.datasource.core.password}")
    private String password;


    @PostConstruct
    public void insertJob() {
        long count = appInfoRepository.count();
        if (count < 1) {
            AppInfoDO appInfoDO = new AppInfoDO();
            appInfoDO.setAppName("aios-app");
            appInfoDO.setCurrentServer("");
            appInfoDO.setGmtCreate(new Date());
            appInfoDO.setGmtModified(new Date());
            String pwd = "introcks1234";
            try {
                pwd = MD5Utils.toMD5("introcks1234");
            } catch (NoSuchAlgorithmException e) {
                log.error("md5加密失败");
            }
            appInfoDO.setPassword(pwd);
            appInfoRepository.save(appInfoDO);

            AppInfoDO appInfoDO1 = new AppInfoDO();
            appInfoDO1.setAppName("aios-app-test");
            appInfoDO1.setCurrentServer("");
            appInfoDO1.setGmtCreate(new Date());
            appInfoDO1.setGmtModified(new Date());
            String pwd1 = "introcks1234";
            try {
                pwd1 = MD5Utils.toMD5("introcks1234");
            } catch (NoSuchAlgorithmException e) {
                log.error("md5加密失败");
            }
            appInfoDO1.setPassword(pwd1);
            appInfoRepository.save(appInfoDO1);
        }

        if (count < 2){
            AppInfoDO appInfoDO1 = new AppInfoDO();
            appInfoDO1.setAppName("aios-app-test");
            appInfoDO1.setCurrentServer("");
            appInfoDO1.setGmtCreate(new Date());
            appInfoDO1.setGmtModified(new Date());
            String pwd1 = "introcks1234";
            try {
                pwd1 = MD5Utils.toMD5("introcks1234");
            } catch (NoSuchAlgorithmException e) {
                log.error("md5加密失败");
            }
            appInfoDO1.setPassword(pwd1);
            appInfoRepository.save(appInfoDO1);
        }

        appInfoRepository.findByAppName("aios-app").ifPresent(byAppName-> {
                    String pwd = "introcks1234";
                    try {
                        pwd = MD5Utils.toMD5("introcks1234");
                    } catch (NoSuchAlgorithmException e) {
                        log.error("md5加密失败");
                    }
                    byAppName.setPassword(pwd);
                    appInfoRepository.save(byAppName);
                }
        );

        appInfoRepository.findByAppName("aios-app-test").ifPresent(byAppName-> {
                    String pwd = "introcks1234";
                    try {
                        pwd = MD5Utils.toMD5("introcks1234");
                    } catch (NoSuchAlgorithmException e) {
                        log.error("md5加密失败");
                    }
                    byAppName.setPassword(pwd);
                    appInfoRepository.save(byAppName);
                }
        );

        List<JobInfoDO> all = jobInfoRepository.findAll();


        boolean flag = CollectionUtils.isEmpty(all);

        List<String> collect = null;

        List<String> list = new ArrayList<>();
        list.add("com.aios.processer.collect.RightNowSnapshotProcess");
        list.add("com.aios.common.cache.CacheJob");
        list.add("com.aios.processer.collect.TimingSnapshotProcess");
        list.add("com.aios.processer.collect.FreeGovernProcess");
        list.add("com.aios.schedule.CameraLabelRelSchedule");

        if (!flag) {
            collect = all.stream().map(jobInfoDO -> {
                if (list.contains(jobInfoDO.getProcessorInfo()) && jobInfoDO.getAppId() == 1){
                    jobInfoDO.setAppId(2L);
                    jobInfoRepository.save(jobInfoDO);
                }else if ("com.aios.schedule.TaskScheduleJob".equals(jobInfoDO.getProcessorInfo())){
                    jobInfoDO.setInstanceTimeLimit(3600000L);
                    jobInfoRepository.save(jobInfoDO);
                }
                return jobInfoDO.getProcessorInfo();
            }).collect(Collectors.toList());
        }



        Connection conn = null;
        Statement stmt = null;

        try {
            Map<String, String> map = createMap();
            Class.forName("com.mysql.cj.jdbc.Driver");
            DriverManager.setLoginTimeout(5);
            conn = DriverManager.getConnection(flywayUrl, username, password);
            stmt = conn.createStatement();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (flag) {
                    stmt.execute(entry.getValue());
                } else {
                    if (!collect.contains(entry.getKey())) {
                        stmt.execute(entry.getValue());
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException se2) {
                se2.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    public Map createMap() {
        Map map = new HashMap<>();
        String local_ip = System.getenv("LOCAL_IP");
        if (!StringUtils.isEmpty(local_ip)) {
            String appId = "power-job";
            try{
                //修改配置
                String url = "http://apollo-portal.middleware:7903/openapi/v1/envs/FAT/apps/" + appId + "/clusters/default/namespaces/application";
                String response = HttpClientUtil.get(url);
                if (!org.apache.commons.lang3.StringUtils.isBlank(response)) {
                    JSONObject jsonObject = JSONObject.parseObject(response);
                    JSONArray data = jsonObject.getJSONArray("items");
                    if (!CollectionUtils.isEmpty(data)) {
                        for (Object o : data){
                            JSONObject json = JSONObject.parseObject(JSONObject.toJSONString(o));
                            String key = json.getString("key");
                            String value = json.getString("value");
                            if (key.startsWith("com.aios")){
                                map.put(key, value);
                            }
                        }
                    }
                }
            }catch (Exception e){
                log.error("http请求失败");
            }
        }
        return map;
    }
}
