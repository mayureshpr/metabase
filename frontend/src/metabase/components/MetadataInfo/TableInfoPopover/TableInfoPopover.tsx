import React from "react";
import PropTypes from "prop-types";
import { connect } from "react-redux";
import { hideAll } from "tippy.js";

import TippyPopover, {
  ITippyPopoverProps,
} from "metabase/components/Popover/TippyPopover";
import Tables from "metabase/entities/tables";
import Table from "metabase-lib/lib/metadata/Table";

import { WidthBoundTableInfo } from "./TableInfoPopover.styled";

export const POPOVER_DELAY: [number, number] = [500, 300];

const mapStateToProps = (
  state: any,
  props: { tableId: number },
): { table?: Table } => {
  return {
    table:
      props.tableId == null
        ? undefined
        : Tables.selectors.getObject(state, {
            entityId: props.tableId,
          }),
  };
};

type Props = { tableId: number } & ReturnType<typeof mapStateToProps> &
  Pick<ITippyPopoverProps, "children" | "placement" | "offset">;

const className = "table-info-popover";

function TableInfoPopover({
  table,
  tableId,
  children,
  placement,
  offset,
}: Props) {
  placement = placement || "left-start";

  const hasDescription = !!table?.description;

  return hasDescription ? (
    <TippyPopover
      className={className}
      interactive
      delay={POPOVER_DELAY}
      placement={placement}
      offset={offset}
      content={<WidthBoundTableInfo tableId={tableId} />}
      onTrigger={instance => {
        const dimensionInfoPopovers = document.querySelectorAll(
          `.${className}[data-state~='visible']`,
        );

        // if a dimension info popover is already visible, hide it and show this one immediately
        if (dimensionInfoPopovers.length > 0) {
          hideAll({
            exclude: instance,
          });
          instance.show();
        }
      }}
    >
      {children}
    </TippyPopover>
  ) : (
    <>{children}</>
  );
}

export default connect(mapStateToProps)(TableInfoPopover);
