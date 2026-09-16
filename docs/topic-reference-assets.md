# 赛道与参考标的（2026-09-15）

每个赛道配置两个参考标的。`config/assets.yaml` 的 `topics` 建立多对多关联；`config/topics.yaml` 的 `reference_assets` 定义显示顺序，第一项为主参考。ETF 和股票的行情分别展示、收益分别计算，不把两只基金平均成虚构指数。研究汇总默认使用主参考，避免一件事件因为配置两个 ETF 而被计为两件。

| 赛道 | 主参考 | 第二参考 |
| --- | --- | --- |
| 黄金 | 518880 华安黄金ETF | 159934 易方达黄金ETF |
| A股大盘 | 510210 富国上证综指ETF | 510760 国泰上证综指ETF |
| 纳斯达克 | QQQ Invesco纳指100ETF | QQQM Invesco NASDAQ 100 ETF |
| AI | 159819 易方达人工智能ETF | 515070 华夏人工智能ETF |
| 人形机器人 | 159770 天弘机器人ETF | 562500 华夏机器人ETF |
| 半导体 | 159813 鹏华半导体ETF | 159801 广发半导体芯片ETF |
| 创新药 | 159992 银华创新药ETF | 515120 广发创新药ETF |
| 新能源 | 516160 南方新能源ETF | 516850 华夏新能源ETF |
| 白酒 | 512690 鹏华酒ETF | 600519 贵州茅台（股票） |
| 恒生科技 | 513180 华夏恒生科技ETF | 159740 大成恒生科技ETF |
| 软件与AI应用 | 515230 国泰软件ETF | 159899 招商软件ETF |
| 消费电子与电子制造 | 159732 华夏消费电子ETF | 159779 招商消费电子ETF |
| 通信设备与光互联 | 515880 国泰通信ETF | 515050 华夏5G通信ETF |
| 工业有色金属 | 560860 万家工业有色ETF | 159162 鹏华工业有色ETF |
| 液冷散热 | 002837 英维克（股票） | 301018 申菱环境（股票） |
| 数据中心电源 | 300870 欧陆通（股票） | 002335 科华数据（股票） |
| 电网设备与特高压 | 159320 电网设备ETF广发 | 159326 电网设备ETF华夏 |
| 军工与航空航天 | 512660 军工ETF国泰 | 159267 航天ETF华安 |
| 智能汽车与自动驾驶 | 159889 智能汽车ETF国泰 | 516520 智能驾驶ETF华泰柏瑞 |
| 油气与石油化工 | 159019 华宝国证石油天然气ETF | 159697 石油ETF鹏华 |

## 覆盖边界

- 消费电子两只基金跟踪不同指数：159732 为国证消费电子主题指数，159779 为中证消费电子主题指数。用作同一产业的两种参考，不声明其持仓完全一致。
- 通信两只基金分别跟踪中证全指通信设备、中证5G通信主题指数，后者覆盖范围更宽。
- 白酒保留一只行业 ETF 加一只观察股票，不用食品饮料 ETF 或场外 LOF 冒充第二只酒类 ETF。
- 液冷、电源的股票价格反映公司经营及市场因素，不等于整个行业指数。
- 上证指数 000001 保留为行情基准；不作为第三个赛道参考标的。QQQ、QQQM 为美元标的，其余参考标的为人民币计价。
- 小红书仍沿用原有六组发现查询；新增股票、ETF 走行情接口，不产生 ETF 数量倍增的小红书搜索。

## 基金及企业资料

- [易方达人工智能159819](https://www.efunds.com.cn/en/fund/159819.shtml)、[华夏人工智能515070](https://www.chinaamc.com/fund_en/515070/index.shtml?source=click)
- [招商软件159899](https://static.cmfchina.com/web/fundDetail/159899/index.html)、[招商消费电子资料](https://www.cmfchina.com/upload/20250401/202504011743468004199.pdf)
- [华夏消费电子159732](https://accountquery.chinaamc.com/fund/159732/xiaoshouwangdian.shtml?source=click)、[华夏5G通信515050](https://fund.chinaamc.com/fund/515050/index.shtml)
- [万家工业有色560860](https://www.wjasset.com/products/etf/560860/index.html)、[鹏华159162等产品公告](https://static.cninfo.com.cn/finalpage/2026-04-27/1225174900.PDF)
- [广发半导体159801](https://www.gffunds.com.cn/funds/?fundcode=159801)、[广发创新药515120](https://www.gffunds.com.cn/funds/?fundcode=515120)
- [华夏机器人562500](https://www.chinaamc.com/fund_en/562500/index.shtml?source=click)、[Invesco纳指系列](https://www.invesco.com/us/en/solutions/innovation-suite.html)
- [英维克](https://www.envicool.com/?lang=cn)、[申菱数据中心液冷](https://www.shenling.com/applications/data-services/idc/)、[欧陆通数据中心电源](https://www.honoto.com/)、[科华数据](https://idc.kehua.com.cn/)

## 历史修复

159869 实际为华夏中证动漫游戏ETF。保留该证券 ID 和真实原始行情，移除错误的 AI 别名及 AI 赛道关联，使用159819、515070真实行情重新计算参考收益。不得把159869旧行情的证券代码直接改成159819。

历史切换只修正参考标的和关联配置，不重采帖子、不调用 LLM、不重新聚合其他赛道的指标、事件或行情。新增赛道从切换日开始积累内容；它们的历史帖子、热度指标和事件保持为空。数据库切换前保留快照，以便回退。
