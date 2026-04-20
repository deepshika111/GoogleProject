args <- commandArgs(trailingOnly = TRUE)

input_path <- if (length(args) >= 1) args[[1]] else "data/processed/ga4_sessions.csv"
output_path <- if (length(args) >= 2) args[[2]] else "reports/channel_experiment_report.md"

if (!file.exists(input_path)) {
  stop(sprintf("Input file not found: %s", input_path))
}

sessions <- read.csv(input_path, stringsAsFactors = FALSE)

required_columns <- c(
  "channel_group",
  "device_category",
  "converted",
  "session_duration_seconds",
  "session_engaged_flag",
  "returning_user_flag",
  "total_product_views",
  "total_add_to_cart_events"
)

missing_columns <- setdiff(required_columns, names(sessions))
if (length(missing_columns) > 0) {
  stop(sprintf("Missing required columns: %s", paste(missing_columns, collapse = ", ")))
}

dir.create(dirname(output_path), recursive = TRUE, showWarnings = FALSE)

normalize_text <- function(values, fallback) {
  values[is.na(values) | trimws(values) == ""] <- fallback
  values
}

write_md_table <- function(dataframe, digits = 3) {
  if (nrow(dataframe) == 0) {
    return("No rows available.\n")
  }

  formatted <- dataframe
  numeric_columns <- sapply(formatted, is.numeric)
  formatted[numeric_columns] <- lapply(formatted[numeric_columns], function(column) round(column, digits))

  header <- paste(names(formatted), collapse = " | ")
  separator <- paste(rep("---", ncol(formatted)), collapse = " | ")
  rows <- apply(formatted, 1, function(row) paste(row, collapse = " | "))
  paste(c(paste0("| ", header, " |"), paste0("| ", separator, " |"), paste0("| ", rows, " |")), collapse = "\n")
}

sessions$channel_group <- normalize_text(sessions$channel_group, "Other")
sessions$device_category <- normalize_text(sessions$device_category, "unknown")
sessions$converted <- as.integer(sessions$converted)
sessions$session_engaged_flag <- as.integer(sessions$session_engaged_flag)
sessions$returning_user_flag <- as.integer(sessions$returning_user_flag)
sessions$total_product_views <- as.numeric(sessions$total_product_views)
sessions$total_add_to_cart_events <- as.numeric(sessions$total_add_to_cart_events)
sessions$session_duration_seconds <- as.numeric(sessions$session_duration_seconds)

channel_counts <- aggregate(
  converted ~ channel_group,
  data = sessions,
  FUN = function(x) c(sessions = length(x), conversions = sum(x), conversion_rate = mean(x))
)
channel_counts <- data.frame(
  channel_group = channel_counts$channel_group,
  sessions = channel_counts$converted[, "sessions"],
  conversions = channel_counts$converted[, "conversions"],
  conversion_rate = channel_counts$converted[, "conversion_rate"],
  row.names = NULL
)
channel_counts <- channel_counts[order(channel_counts$sessions, decreasing = TRUE), ]

eligible_channels <- subset(channel_counts, sessions >= 100)
baseline_channel <- if (nrow(eligible_channels) > 0) eligible_channels$channel_group[[1]] else channel_counts$channel_group[[1]]

pairwise_results <- data.frame()

if (nrow(eligible_channels) > 1) {
  baseline_rows <- subset(sessions, channel_group == baseline_channel)
  baseline_conversions <- sum(baseline_rows$converted)
  baseline_sessions <- nrow(baseline_rows)

  for (channel in eligible_channels$channel_group) {
    if (channel == baseline_channel) {
      next
    }

    comparison_rows <- subset(sessions, channel_group == channel)
    comparison_conversions <- sum(comparison_rows$converted)
    comparison_sessions <- nrow(comparison_rows)
    test_result <- prop.test(
      x = c(baseline_conversions, comparison_conversions),
      n = c(baseline_sessions, comparison_sessions),
      correct = FALSE
    )

    pairwise_results <- rbind(
      pairwise_results,
      data.frame(
        baseline_channel = baseline_channel,
        comparison_channel = channel,
        baseline_rate = baseline_conversions / baseline_sessions,
        comparison_rate = comparison_conversions / comparison_sessions,
        rate_difference = (comparison_conversions / comparison_sessions) - (baseline_conversions / baseline_sessions),
        p_value = test_result$p.value,
        confidence_low = test_result$conf.int[[1]],
        confidence_high = test_result$conf.int[[2]]
      )
    )
  }
}

chi_square_result <- NULL
if (nrow(eligible_channels) >= 2) {
  contingency_table <- xtabs(
    ~ channel_group + converted,
    data = subset(sessions, channel_group %in% eligible_channels$channel_group)
  )
  chi_square_result <- suppressWarnings(chisq.test(contingency_table))
}

logit_model <- glm(
  converted ~ channel_group + device_category + session_duration_seconds +
    session_engaged_flag + returning_user_flag + total_product_views + total_add_to_cart_events,
  data = sessions,
  family = binomial()
)

logit_summary <- summary(logit_model)$coefficients
logit_table <- data.frame(
  term = rownames(logit_summary),
  estimate = logit_summary[, "Estimate"],
  odds_ratio = exp(logit_summary[, "Estimate"]),
  p_value = logit_summary[, "Pr(>|z|)"],
  row.names = NULL
)
logit_table <- logit_table[order(logit_table$p_value), ]

significant_terms <- subset(logit_table, p_value < 0.05)
if (nrow(significant_terms) == 0) {
  significant_terms <- head(logit_table, 5)
}

sink(output_path)

cat("# Channel Experiment Analysis\n\n")
cat(sprintf("- Input: `%s`\n", input_path))
cat(sprintf("- Sessions analyzed: %s\n", format(nrow(sessions), big.mark = ",")))
cat(sprintf("- Baseline channel for pairwise testing: `%s`\n\n", baseline_channel))

cat("## Channel Summary\n\n")
cat(write_md_table(channel_counts, digits = 4))
cat("\n\n")

cat("## Pairwise Conversion Tests\n\n")
if (nrow(pairwise_results) == 0) {
  cat("Not enough eligible channels to run pairwise `prop.test` comparisons.\n\n")
} else {
  cat(write_md_table(pairwise_results, digits = 5))
  cat("\n\n")
}

cat("## Chi-Square Test\n\n")
if (is.null(chi_square_result)) {
  cat("Not enough eligible channels to run the chi-square test.\n\n")
} else {
  cat(sprintf(
    "Chi-square statistic = %.4f, degrees of freedom = %s, p-value = %.6f\n\n",
    chi_square_result$statistic,
    chi_square_result$parameter,
    chi_square_result$p.value
  ))
}

cat("## Logistic Regression Drivers\n\n")
cat(write_md_table(significant_terms, digits = 5))
cat("\n\n")

cat("## Interpretation Template\n\n")
cat(
  paste(
    "1. Lead with the biggest conversion gap versus the baseline channel and note whether the `prop.test` result is statistically significant.",
    "2. Use the chi-square result to say whether purchase propensity differs across channels overall.",
    "3. Use the logistic regression output to explain whether channel effects remain after controlling for device, engagement, session duration, and returning-user behavior.",
    "4. Translate the best-performing segments into a recommendation such as reallocating budget, improving mobile funnel UX, or tightening landing-page alignment for weaker channels.",
    sep = "\n"
  )
)
cat("\n")

sink()

message(sprintf("Report written to %s", output_path))
