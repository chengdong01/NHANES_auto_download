library(nhanesA)
library(purrr)
library(httr)
library(dplyr)
library(haven)
library(stringr)
download_nhanes_table <- function(
    table_name, 
    save_dir = getwd(), 
    max_retries = 5,
    check_vars = c("SEQN"),
    save_files = TRUE,
    verbose = TRUE
) {
  # 配置全局参数
  options(
    download.file.method = "libcurl",
    download.file.extra = "--insecure --retry 3 --retry-delay 5"
  )
  set_config(config(ssl_verifypeer = FALSE, ssl_verifyhost = FALSE))
  
  # 创建保存目录
  dir.create(save_dir, showWarnings = FALSE, recursive = TRUE)
  
  # 生成保存路径
  rds_path <- file.path(save_dir, paste0(table_name, ".rds"))
  csv_path <- file.path(save_dir, paste0(table_name, ".csv"))
  xpt_path <- file.path(save_dir, paste0(table_name, ".xpt"))
  # 核心下载逻辑
  result <- NULL
  attempt <- 1
  
  while (is.null(result) && attempt <= max_retries) {
    message("\n=== 尝试下载 ", table_name, " (第 ", attempt, "/", max_retries, " 次) ===")
    
    # 尝试下载
    data <- tryCatch(
      expr = {
        df <- nhanes(table_name)
        # 数据有效性检查
        if (!all(check_vars %in% names(df))) 
          stop("缺少关键列: ", paste(setdiff(check_vars, names(df)), collapse = ", "))
        if (nrow(df) == 0) 
          stop("数据为空")
        message("✅ 数据有效性验证通过 | 样本量: ", nrow(df))
        df
      },
      error = function(e) {
        message("❌ 下载失败: ", e$message)
        Sys.sleep(5 * attempt)  # 指数退避等待
        NULL
      }
    )
    
    # 保存数据
    if (!is.null(data)) {
      # saveRDS(data, rds_path)
      write_xpt(data, xpt_path, version = 8)
      # write.csv(data, csv_path, row.names = FALSE)
      message("📁 数据已保存: \n  ", rds_path, "\n  ", csv_path)
    }
    else {
      if (attempt == max_retries) {
        # 创建失败记录目录
        failed_dir <- file.path(save_dir, "failed_downloads")
        if (!dir.exists(failed_dir)) {
          dir.create(failed_dir, recursive = TRUE)
        }
        
        # 生成带时间戳的文件名
        timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
        failed_file <- file.path(failed_dir, 
                                 paste0("failed_tables_", timestamp, ".txt"))
        
        # 打印到控制台
        message("\n❌ 达到最大重试次数，以下表格下载失败: ", table_name)
        
        # 写入文件(追加模式)
        write.table(data.frame(table_name = table_name,
                               timestamp = Sys.time()),
                    file = failed_file,
                    append = file.exists(failed_file),  # 如果文件存在则追加
                    col.names = !file.exists(failed_file),  # 首次写入包含列名
                    row.names = FALSE,
                    sep = "\t")
        
        # 生成汇总报告
        generate_summary_report(failed_dir)
      }
    }
    attempt <- attempt + 1
    result <- data
  }
  
  # 最终处理
  if (is.null(result)) {
    # 生成手动下载指南
    cycle_code <- substr(table_name, nchar(table_name)-1, nchar(table_name))
    manual_url <- paste0(
      "https://wwwn.cdc.gov/Nchs/Nhanes/",
      ifelse(grepl("_", table_name), 
             paste0(gsub("_.*", "", table_name), "/", table_name, ".XPT"), 
             paste0(table_name, ".XPT"))  # 修正参数顺序
    )
    
    warning(paste0(
      "\n⚠️ 自动下载失败，请手动操作:\n",
      "1. 访问: ", manual_url, "\n",
      "2. 下载文件至: ", save_dir, "/", table_name, ".XPT\n",
      "3. 加载本地文件: read_nhanes_local('", table_name, ".XPT')"
    ))
  }
  
  return(invisible(result))
}
# 生成汇总报告的辅助函数
generate_summary_report <- function(failed_dir) {
  # 读取所有失败记录
  failed_files <- list.files(failed_dir, pattern = "\\.txt$", full.names = TRUE)
  
  if (length(failed_files) > 0) {
    # 合并历史记录
    all_failed <- do.call(rbind, 
                          lapply(failed_files, function(f) {
                            read.delim(f, stringsAsFactors = FALSE)
                          }))
    
    # 生成统计摘要
    summary_stats <- all_failed %>%
      group_by(table_name) %>%
      summarise(
        failure_count = n(),
        last_attempt = max(timestamp)
      )
    
    # 保存汇总报告
    summary_file <- file.path(failed_dir, "download_failure_summary.csv")
    write.csv(summary_stats, summary_file, row.names = FALSE)
    
    message("📊 失败汇总报告已生成: ", summary_file)
  }
}
# download_nhanes_table("DEMO_H",max_retries = 10,save_dir = "G:/NHANES_DATA",verbose = FALSE)
# 辅助函数：加载手动下载的文件
read_nhanes_local <- function(xpt_path, check_vars = c("SEQN")) {
  library(haven)
  data <- read_xpt(xpt_path)
  
  if (!all(check_vars %in% names(data))) 
    warning("本地文件缺少关键列: ", paste(setdiff(check_vars, names(data)), collapse = ", "))
  if (nrow(data) == 0) 
    warning("本地文件数据为空")
  
  return(data)
}
batch_download_nhanes <- function(cycles, modules, save_root = "NHANES_Data") {
  # 遍历所有周期和模块
  for (cycle_name in names(cycles)) {
    cycle_code <- cycles[[cycle_name]]
    
    # 创建周期目录 (例如: NHANES_Data/2001-2002)
    save_dir <- file.path(save_root, cycle_name)
    
    # 遍历模块
    for (mod_name in names(modules)) {
      mod_prefix <- modules[[mod_name]]
      
      # 生成表名 (例如: DEMO_B, BPX_B)
      # table_name <- paste0(mod_prefix, "_", cycle_code)
      # full_path <- file.path(save_dir, table_name)
      table_name <- paste0(mod_prefix, "_", cycle_code)  # 生成原始表名（如DEMO_H）
      xpt_path <- file.path(save_dir, paste0(table_name, ".xpt"))
      # full_path <- file.path(save_dir, table_name)       # 路径未包含扩展名和格式化后的文件名
      if (file.exists(xpt_path)) {
        message(sprintf("\n>>>>> [跳过] 文件已存在: %s/%s", cycle_name, xpt_path))
        next  # 跳过本次循环
      }
      # 下载并保存数据
      message("\n>>>>> 正在下载: ", cycle_name, " - ", mod_name, " (", table_name, ")")
      data <- download_nhanes_table(
        table_name = table_name,
        save_dir = save_dir,
        check_vars = "SEQN",  # 必须包含SEQN列
        max_retries = 5
      )
      
      # 记录下载状态
      if (!is.null(data)) {
        message("成功下载 ", mod_name, " (样本量: ", nrow(data), ")")
      } else {
        message("!! 下载失败: ", table_name)
      }
    }
  }
}

copy_nhanes_data <- function(source_dir = "C:/Always/NHANES_download/NHANES_Data",
                             target_dir = "C:/OBS_score/data",
                             overwrite = FALSE) {
  # 创建目标目录
  if (!dir.exists(target_dir)) {
    dir.create(target_dir, recursive = TRUE, showWarnings = FALSE)
    message("创建目标目录: ", target_dir)
  }
  
  # 获取所有需要复制的文件路径
  all_files <- list.files(
    path = source_dir,
    pattern = "\\.(xpt|csv|rds)$", 
    recursive = TRUE,
    full.names = TRUE,
    ignore.case = TRUE
  )
  
  if (length(all_files) == 0) {
    message("⚠️ 源目录无数据文件: ", source_dir)
    return(FALSE)
  }
  
  # 执行批量复制
  copy_results <- lapply(all_files, function(f) {
    rel_path <- substring(f, nchar(source_dir) + 2)
    dest <- file.path(target_dir, rel_path)
    
    # 创建子目录
    dest_dir <- dirname(dest)
    if (!dir.exists(dest_dir)) {
      dir.create(dest_dir, recursive = TRUE, showWarnings = FALSE)
    }
    
    # 执行复制
    tryCatch({
      if (file.copy(f, dest, overwrite = overwrite)) {
        message("成功复制: ", rel_path)
        return(list(status = "success", file = rel_path))
      } else {
        message("❌ 复制失败: ", rel_path)
        return(list(status = "fail", file = rel_path))
      }
    }, error = function(e) {
      message("❗ 复制异常: ", rel_path, " - ", e$message)
      return(list(status = "error", file = rel_path))
    })
  })
  
  # 生成复制报告
  report <- do.call(rbind, lapply(copy_results, as.data.frame))
  csv_path <- file.path(target_dir, "data_copy_report.csv")
  write.csv(report, csv_path, row.names = FALSE)
  message("\n复制报告已保存: ", csv_path)
  
  # 返回是否全部成功
  all(sapply(copy_results, function(x) x$status == "success"))
}
rename_nhanes_files <- function(dir_path = "NHANES_Data") {
  # 加载必要包
  library(stringr)
  
  # 周期代码映射表
  cycle_map <- c(
    "B" = "2001-2002",
    "C" = "2003-2004",
    "D" = "2005-2006",
    "E" = "2007-2008",
    "F" = "2009-2010",
    "G" = "2011-2012",
    "H" = "2013-2014",
    "I" = "2015-2016",
    "J" = "2017-2018"
  )
  
  # 模块名称映射表（扩展更多模块）
  module_map <- list(
    DEMO    = "Demographic_Variables_and_Sample_Weights",
    BPX     = "Blood Pressure",
    BMX     = "BMX",
    TCHOL   = "Cholesterol - Total",
    HDL     = "Cholesterol - High - Density Lipoprotein",
    TRIGLY  = "Cholesterol - LDL & Triglycerides",
    PAQ     = "Physical Activity",
    DR1TOT  = "Dietary_Interview-Total_Nutrient_Intakes_First_Day",
    GHB     = "Glycohemoglobin",
    INS     = "Insulin",
    MCQ     = "Medical Conditions",
    OGTT    = "Oral Glucose Tolerance Test",
    GLU     = "Plasma Fasting Glucose",
    RHQ     = "Reproductive Health",
    BIOPRO  = "Standard Biochemistry Profile",
    HEPC    = "Hepatitis C Confirmed Antibody",
    COT     = "COT",
    LUX     = "Liver Ultrasound Transient Elastography",
    
    # 新增基于文件名的特殊映射
    BPQ     = "Blood Pressure & Cholesterol",       # 血压和胆固醇联合数据
    GGT     = "Standard Biochemistry Profile",# 匹配GGT_file变量
    RHQ     = "Reproductive Health"
  )
 
  
  # 递归获取所有XPT/CSV文件（不区分大小写）
  files <- list.files(
    dir_path,
    pattern = "\\.(xpt|csv)$", 
    recursive = TRUE,   # 搜索子目录
    full.names = TRUE,
    ignore.case = TRUE  # 忽略扩展名大小写
  )
  
  if (length(files) == 0) {
    message("未找到任何.xpt或.csv文件")
    return(invisible(NULL))
  }
  
  # 处理每个文件
  results <- lapply(files, function(f) {
    tryCatch({
      # 提取基本信息
      original_name <- basename(f)
      file_dir <- dirname(f)
      base_name <- tools::file_path_sans_ext(original_name)
      ext <- tools::file_ext(original_name)
      
      # 调试输出
      message("\n处理文件: ", original_name)
      
      # 解析表名（支持两种格式：DEMO_H 或 DEMO-2013-2014）
      if (str_detect(base_name, "_[A-Z]$")) {
        # 格式: DEMO_H
        suffix <- str_sub(base_name, -1, -1)
        prefix <- str_replace(base_name, "_[A-Z]$", "")
      } else if (str_detect(base_name, "-\\d{4}-\\d{4}$")) {
        # 格式: 2013-2014-Demographics (跳过已处理文件)
        message("文件已符合新命名规范，跳过")
        return(NULL)
      } else {
        stop("无法识别的文件名格式")
      }
      
      # 获取周期和模块名称
      cycle <- cycle_map[suffix]
      if (is.na(cycle)) {
        stop("未知周期代码: ", suffix)
      }
      
      module <- if (prefix %in% names(module_map)) {
        module_map[[prefix]]
      } else {
        # 自动生成友好名称（将_替换为空格并首字母大写）
        str_replace_all(prefix, "_", " ") %>%
          str_to_title()
      }
      
      # 生成新文件名
      new_base <- paste(cycle, module, sep = "-")
      new_base_safe <- str_remove_all(new_base, "[\\\\/:*?\"<>|]")  # 移除非法字符
      new_file <- paste0(new_base_safe, ".", tolower(ext))
      new_path <- file.path(file_dir, new_file)
      
      # 执行重命名
      if (file.rename(f, new_path)) {
        message("重命名成功: ", original_name, " -> ", new_file)
        return(data.frame(
          original = original_name,
          new = new_file,
          status = "Success",
          stringsAsFactors = FALSE
        ))
      } else {
        stop("文件操作失败")
      }
    }, error = function(e) {
      message("!! 错误: ", conditionMessage(e))
      return(data.frame(
        original = original_name,
        new = NA,
        status = paste("Error:", e$message),
        stringsAsFactors = FALSE
      ))
    })
  })
  
  # 生成汇总报告
  report <- do.call(rbind, results)
  csv_path <- file.path(dir_path, "rename_report.csv")
  write.csv(report, csv_path, row.names = FALSE)
  message("\n重命名报告已保存至: ", csv_path)
  
  return(invisible(report))
}

# 使用示例
rename_nhanes_files("G:/NHANES_DATA")
# 
cycles <- list(
  `2001-2002` = "B",
  `2003-2004` = "C",
  `2005-2006` = "D",
  `2007-2008` = "E",
  `2009-2010` = "F",
  `2011-2012` = "G",
  `2013-2014` = "H",
  `2015-2016` = "I",
  `2017-2018` = "J"
)

# 定义目标模块及表名前缀
modules <- list(
  demographics = "DEMO",              # 人口统计
  blood_pressure = "BPX",             # 血压
  body_measures = "BMX",             # 身体测量
  cholesterol_total = "TCHOL",       # 总胆固醇
  cholesterol_hdl = "HDL",           # 高密度脂蛋白
  triglycerides = "TRIGLY",          # 甘油三酯
  diabetes = "DIQ",                  # 糖尿病
  diet = "DR1TOT",                   # 膳食调查
  glycohemoglobin = "GHB",           # 糖化血红蛋白
  cotinine = "COT",                  # 可替宁
  # insulin = "INS",                   # 胰岛素
  # liver_ultrasound = "LUX",          # 肝脏超声
  physical_activity = "PAQ",         # 体力活动
  biochemistry = "BIOPRO",           # 生化指标
  hepatitis = "HEPC",                # 肝炎
  # ======== 新增模块 ========
  oral_glucose_tolerance = "OGTT",    # 口服葡萄糖耐量试验
  medical_conditions = "MCQ",         # 医疗状况
  Blood_Pressure_Cholesterol ="BPQ",
  fasting_glucose = "GLU",          # 空腹血糖
  reproductive_health = "RHQ"         # 生殖健康
)
download_dir = "C:/Always/NHANES_download/NHANES_Data"
rename_dir = "C:/Always/NHANES_download/NHANES_Data(process)"
setwd(download_dir)
batch_download_nhanes(cycles, modules)
if (copy_nhanes_data(source_dir = download_dir,
                     target_dir = rename_dir)) {
  message("\n数据复制完成，开始重命名操作...")
  rename_nhanes_files(rename_dir)
} else {
  warning("存在未成功复制的文件，请检查报告后再执行重命名")
}
