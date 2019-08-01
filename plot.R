library(plyr)
library(ggplot2)

ticks = c(c(1), seq(5,100, by=5))
# TODO: color blind friendly palette
color_key = c("#000000", "#2080FF", "#FF80B0")

# data_structures <- c("List", "HashMap", "NMTree", "BonsaiTree")
data_structures <- c("HashMap")
for (ds in data_structures) {
  read.csv(paste(ds, "_results.csv", sep = "")) -> data

  # TODO: use avg or max of several runs
  ddply(.data=data,.(mm,threads),mutate,throughput= max(throughput)/1000000)->data

  data$mm <- factor(data$mm, levels = c("NoMM", "EBR", "PEBR"))

  # Set up colors and shapes (invariant for all plots)
  names(color_key) <- unique(c(as.character(data$mm)))

  shape_key = c(1, 2, 3)
  names(shape_key) <- unique(c(as.character(data$mm)))

  line_key = c(1, 2, 3)
  names(line_key) <- unique(c(as.character(data$mm)))

  ## Throughput

  # legend_pos = c(0.4, 0.92)
  # y_range_down = 0
  # y_range_up = 100

  # if (ds == "BonsaiTree") {
  #   y_range_down = 0.07
  #   legend_pos = c(0.5, 0.92)
  #   y_range_up = 0.30
  # } else if (ds == "List") {
  #   y_range_down = 0
  #   y_range_up = 0.045
  # } else if (ds == "NMTree") {
  #   y_range_up = 45
  # } else if (ds == "HashMap") {
  #   legend_pos = c(0.33, 0.92)
  # }

  # Generate the plots
  throughput_chart <- ggplot(
    data = data,
    aes(
      x = threads,
      y = throughput,
      color = mm,
      shape = mm,
      linetype = mm
    )
  ) +
    geom_line() + xlab("Threads") + ylab("Throughput (M ops/s)") + geom_point(size = 4) #+
    # scale_shape_manual(values = shape_key[names(shape_key) %in% data$mm]) +
    # scale_linetype_manual(values = line_key[names(line_key) %in% data$mm]) +
    # theme_bw() + guides(shape = guide_legend(title = NULL, nrow = 2)) +
    # guides(color = guide_legend(title = NULL, nrow = 2)) +
    # guides(linetype = guide_legend(title = NULL, nrow = 2)) +
    # scale_color_manual(values = color_key[names(color_key) %in% data$mm]) +
    # scale_x_continuous(
    #   breaks = ticks,
    #   minor_breaks = ticks
    # ) +
    # theme(plot.margin = unit(c(.2, 0, .2, 0), "cm")) +
    # theme(legend.position = legend_pos,
    #       legend.direction = "horizontal") +
    # theme(text = element_text(size = 20)) +
    # theme(axis.title.y = element_text(margin = margin(
    #   t = 0,
    #   r = 15,
    #   b = 0,
    #   l = 10
    # ))) +
    # theme(axis.title.x = element_text(margin = margin(
    #   t = 15,
    #   r = 0,
    #   b = 10,
    #   l = 0
    # ))) +
    # ylim(y_range_down, y_range_up)

  # Save all four plots to separate PDFs
  ggsave(
    filename = paste(ds, "_throughput.pdf", sep = ""),
    throughput_chart,
    width = 8,
    height = 5.5,
    units = "in",
    dpi = 300
  )

  unreclaimed_chart <- ggplot(
    data = data,
    aes(
      x = threads,
      y = avg_unreclaimed,
      color = mm,
      shape = mm,
      linetype = mm
    )
  ) +
    geom_line() + xlab("Threads") + ylab("Average unreclaimed blocks") + geom_point(size = 4) #+
  ggsave(
    filename = paste(ds, "_avg_unreclaimed.pdf", sep = ""),
    unreclaimed_chart,
    width = 8,
    height = 5.5,
    units = "in",
    dpi = 300
  )
}

