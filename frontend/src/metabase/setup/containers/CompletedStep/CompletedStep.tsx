import { connect } from "react-redux";
import CompletedStep from "../../components/CompletedStep";
import { COMPLETED_STEP } from "../../constants";
import { isStepActive } from "../../selectors";

const mapStateToProps = (state: any) => ({
  isStepActive: isStepActive(state, COMPLETED_STEP),
});

export default connect(mapStateToProps)(CompletedStep);
