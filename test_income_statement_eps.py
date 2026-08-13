from income_statement_service import _append_missing_eps_facts


def test_recovers_eps_fact_dropped_by_lxml_html_parser():
    html = b"""
    <table>
      <tr>
        <td>Total basic earnings per share</td>
        <td><ix:nonFraction
          name="ifrs-full:BasicEarningsLossPerShare"
          contextRef="From20260401To20260630"
          scale="0"
          decimals="2"
          unitRef="EarningsPerShare">1.06</ix:nonFraction></td>
        <td><ix:nonFraction
          name="ifrs-full:BasicEarningsLossPerShare"
          contextRef="From20260101To20260630"
          scale="0"
          decimals="2"
          unitRef="EarningsPerShare">2.15</ix:nonFraction></td>
      </tr>
    </table>
    """
    results = []

    _append_missing_eps_facts(results, html)

    assert [item["value_text"] for item in results] == ["1.06", "2.15"]
    assert results[0]["contextref"] == "From20260401To20260630"
    assert results[1]["contextref"] == "From20260101To20260630"
