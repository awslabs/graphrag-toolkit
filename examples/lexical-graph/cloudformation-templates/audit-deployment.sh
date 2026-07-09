#!/usr/bin/env bash
#
# audit-deployment.sh - Comprehensive audit script for GraphRAG Toolkit CloudFormation deployments
#
# This script audits all resources deployed by graphrag-toolkit CloudFormation templates.
# It checks resource existence, status, configuration, and cost-relevant details.
#
# Supported template variants:
#   - neptune-analytics (with opensearch, s3-vectors, or aurora-postgres)
#   - neptune-db (with opensearch, s3-vectors, or aurora-postgres)
#   - neptune-db-aurora-postgres-existing-vpc
#
# Usage:
#   ./audit-deployment.sh [--stack-name NAME] [--region REGION] [--profile PROFILE] [--json]
#
# Options:
#   --stack-name   CloudFormation stack name (auto-detects if not provided)
#   --region       AWS region (default: us-east-1)
#   --profile      AWS CLI profile (optional)
#   --json         Output in JSON format for machine-readable consumption
#   --help         Show this help message
#
# Author: Generated for graphrag-toolkit
# Date: 2024
#

set -euo pipefail

###############################################################################
# CONFIGURATION & GLOBALS
###############################################################################

VERSION="1.1.0"
STACK_NAME=""
REGION="us-east-1"
PROFILE=""
JSON_OUTPUT=false
MANUAL_MODE=false
RESOURCE_CONFIG=""
AWS_CMD="aws"

# Counters for summary
RESOURCES_FOUND=0
RESOURCES_EXPECTED=0
RESOURCES_HEALTHY=0
RESOURCES_UNHEALTHY=0
RESOURCES_UNKNOWN=0

# JSON accumulator (stored as a simple variable for bash 3 compat)
JSON_SECTION_CLOUDFORMATION="{}"

###############################################################################
# COLOR DEFINITIONS
###############################################################################

if [[ -t 1 ]] && [[ "${NO_COLOR:-}" != "1" ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    MAGENTA='\033[0;35m'
    BOLD='\033[1m'
    DIM='\033[2m'
    RESET='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' MAGENTA='' BOLD='' DIM='' RESET=''
fi

###############################################################################
# UTILITY FUNCTIONS
###############################################################################

print_header() {
    local title="$1"
    if [[ "$JSON_OUTPUT" == "true" ]]; then return; fi
    echo ""
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    echo -e "${BOLD}${BLUE}  $title${RESET}"
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
}

print_subheader() {
    local title="$1"
    if [[ "$JSON_OUTPUT" == "true" ]]; then return; fi
    echo ""
    echo -e "  ${BOLD}${CYAN}▸ $title${RESET}"
    echo -e "  ${DIM}──────────────────────────────────────────────────────────${RESET}"
}

print_status() {
    local label="$1"
    local value="$2"
    local status="${3:-info}"  # info, ok, warn, error
    if [[ "$JSON_OUTPUT" == "true" ]]; then return; fi
    
    local color=""
    local icon=""
    case "$status" in
        ok)    color="$GREEN"; icon="✓" ;;
        warn)  color="$YELLOW"; icon="⚠" ;;
        error) color="$RED"; icon="✗" ;;
        info)  color="$CYAN"; icon="ℹ" ;;
    esac
    printf "    ${color}${icon}${RESET} %-35s %s\n" "$label:" "$value"
}

print_table_row() {
    local col1="$1" col2="$2" col3="${3:-}" col4="${4:-}"
    if [[ "$JSON_OUTPUT" == "true" ]]; then return; fi
    if [[ -n "$col4" ]]; then
        printf "    %-30s %-20s %-25s %s\n" "$col1" "$col2" "$col3" "$col4"
    elif [[ -n "$col3" ]]; then
        printf "    %-30s %-25s %s\n" "$col1" "$col2" "$col3"
    else
        printf "    %-35s %s\n" "$col1" "$col2"
    fi
}

print_table_header() {
    if [[ "$JSON_OUTPUT" == "true" ]]; then return; fi
    local cols=("$@")
    local header=""
    local separator=""
    for col in "${cols[@]}"; do
        header+=$(printf "%-30s " "$col")
        separator+=$(printf "%-30s " "$(printf '%0.s─' $(seq 1 ${#col}))")
    done
    echo -e "    ${BOLD}${header}${RESET}"
    echo -e "    ${DIM}${separator}${RESET}"
}

status_color() {
    local status="$1"
    case "$status" in
        *COMPLETE*|*AVAILABLE*|*ACTIVE*|*InService*|*running*|*available*)
            echo -e "${GREEN}${status}${RESET}" ;;
        *PROGRESS*|*UPDATING*|*CREATING*|*starting*|*pending*)
            echo -e "${YELLOW}${status}${RESET}" ;;
        *FAILED*|*DELETE*|*stopped*|*error*|*INACTIVE*)
            echo -e "${RED}${status}${RESET}" ;;
        *)
            echo -e "${status}" ;;
    esac
}

# Execute AWS CLI command with proper profile/region
aws_cmd() {
    local cmd="$AWS_CMD"
    if [[ -n "$PROFILE" ]]; then
        cmd+=" --profile $PROFILE"
    fi
    cmd+=" --region $REGION"
    cmd+=" $*"
    eval "$cmd" 2>/dev/null
}

# Safe JSON extraction
json_get() {
    local json="$1"
    local query="$2"
    echo "$json" | jq -r "$query // empty" 2>/dev/null || echo ""
}

###############################################################################
# ARGUMENT PARSING
###############################################################################

show_help() {
    cat << EOF
${BOLD}GraphRAG Toolkit Deployment Audit Script v${VERSION}${RESET}

Usage: $(basename "$0") [OPTIONS]

Options:
    --stack-name NAME    CloudFormation stack name (auto-detects 'graphrag' stacks)
    --region REGION      AWS region (default: us-east-1)
    --profile PROFILE    AWS CLI profile name
    --manual             Scan for resources directly (no CloudFormation stack required)
    --resources FILE     JSON config file for manual mode (default: audit-resources.json)
    --json               Output in JSON format
    --help               Show this help message

Examples:
    $(basename "$0")
    $(basename "$0") --stack-name my-graphrag-stack --region us-west-2
    $(basename "$0") --profile production --json
    $(basename "$0") --manual --region us-east-1
    $(basename "$0") --manual --profile production

EOF
    exit 0
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --stack-name)
                STACK_NAME="$2"
                shift 2
                ;;
            --region)
                REGION="$2"
                shift 2
                ;;
            --profile)
                PROFILE="$2"
                shift 2
                ;;
            --manual)
                MANUAL_MODE=true
                shift
                ;;
            --resources)
                RESOURCE_CONFIG="$2"
                MANUAL_MODE=true
                shift 2
                ;;
            --json)
                JSON_OUTPUT=true
                shift
                ;;
            --help|-h)
                show_help
                ;;
            *)
                echo -e "${RED}Error: Unknown option: $1${RESET}" >&2
                echo "Use --help for usage information." >&2
                exit 1
                ;;
        esac
    done
}


###############################################################################
# STACK DETECTION
###############################################################################

detect_stack() {
    if [[ -n "$STACK_NAME" ]]; then
        return 0
    fi

    if [[ "$JSON_OUTPUT" != "true" ]]; then
        echo -e "${YELLOW}No --stack-name provided. Searching for GraphRAG stacks...${RESET}"
    fi

    local stacks
    stacks=$(aws_cmd cloudformation list-stacks \
        --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE UPDATE_ROLLBACK_COMPLETE \
        --query "StackSummaries[?contains(StackName, 'graphrag') || contains(StackName, 'GraphRAG')].{Name:StackName,Status:StackStatus,Created:CreationTime}" \
        --output json 2>/dev/null || echo "[]")

    local count
    count=$(echo "$stacks" | jq 'length' 2>/dev/null || echo "0")

    if [[ "$count" -eq 0 ]]; then
        echo -e "${RED}Error: No GraphRAG stacks found in ${REGION}.${RESET}" >&2
        echo "Use --stack-name to specify a stack explicitly." >&2
        exit 1
    elif [[ "$count" -eq 1 ]]; then
        STACK_NAME=$(echo "$stacks" | jq -r '.[0].Name')
        if [[ "$JSON_OUTPUT" != "true" ]]; then
            echo -e "${GREEN}Auto-detected stack: ${BOLD}${STACK_NAME}${RESET}"
        fi
    else
        if [[ "$JSON_OUTPUT" != "true" ]]; then
            echo -e "${YELLOW}Multiple GraphRAG stacks found:${RESET}"
            echo "$stacks" | jq -r '.[] | "  - \(.Name) [\(.Status)] (created: \(.Created))"'
            echo ""
            echo -e "${YELLOW}Please specify one with --stack-name${RESET}"
        fi
        exit 1
    fi
}

###############################################################################
# MANUAL RESOURCE DISCOVERY (no CloudFormation stack required)
###############################################################################

discover_resources_manual() {
    # In manual mode, we scan AWS directly by RESOURCE TYPE.
    # Reads resource types from audit-resources.json config file.

    # Locate config file (same directory as script)
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local config_file="${RESOURCE_CONFIG:-${script_dir}/audit-resources.json}"

    if [[ ! -f "$config_file" ]]; then
        echo -e "${RED}Error: Resource config not found: ${config_file}${RESET}" >&2
        echo "Create audit-resources.json or specify with --resources <path>" >&2
        exit 1
    fi

    if [[ "$JSON_OUTPUT" != "true" ]]; then
        print_header "MANUAL RESOURCE DISCOVERY"
        echo -e "    ${YELLOW}Config: ${config_file}${RESET}"
        echo -e "    ${YELLOW}Scanning region ${REGION} for enabled resource types...${RESET}"
        echo ""
    fi

    local resources="[]"

    # Read enabled resource types from config
    local enabled_types
    enabled_types=$(jq -c '.resource_types[] | select(.enabled == true)' "$config_file" 2>/dev/null)

    if [[ -z "$enabled_types" ]]; then
        echo -e "${RED}Error: No enabled resource types in ${config_file}${RESET}" >&2
        exit 1
    fi

    # Iterate over each enabled resource type
    while IFS= read -r entry; do
        [[ -z "$entry" ]] && continue

        local label service operation jq_extract logical_id cfn_type rtype
        label=$(echo "$entry" | jq -r '.label')
        service=$(echo "$entry" | jq -r '.service')
        operation=$(echo "$entry" | jq -r '.operation')
        jq_extract=$(echo "$entry" | jq -r '.jq_extract')
        logical_id=$(echo "$entry" | jq -r '.logical_id')
        cfn_type=$(echo "$entry" | jq -r '.cfn_type')
        rtype=$(echo "$entry" | jq -r '.type')

        if [[ "$JSON_OUTPUT" != "true" ]]; then
            printf "    Scanning %-40s\r" "${label}..."
        fi

        # Execute the AWS CLI call
        local result
        local cmd="$AWS_CMD"
        [[ -n "$PROFILE" ]] && cmd+=" --profile $PROFILE"
        cmd+=" --region $REGION"
        cmd+=" $service $operation --output json"
        result=$(eval "$cmd" 2>/dev/null || echo "{}")

        # Extract resource IDs using the jq expression from config
        while IFS= read -r rid; do
            [[ -z "$rid" ]] && continue
            resources=$(echo "$resources" | jq --arg id "$rid" --arg lid "$logical_id" --arg rt "$cfn_type" \
                '. += [{"LogicalResourceId":$lid,"PhysicalResourceId":$id,"ResourceType":$rt,"ResourceStatus":"DISCOVERED"}]')
        done < <(echo "$result" | jq -r "$jq_extract" 2>/dev/null)

        # Special handling: VPC also discovers subnets, SGs, NAT/IGW, endpoints
        if [[ "$rtype" == "vpc" ]]; then
            # For each discovered VPC, get associated networking resources
            while IFS= read -r vid; do
                [[ -z "$vid" ]] && continue

                # Subnets
                local subnets
                subnets=$(aws_cmd ec2 describe-subnets --filters "Name=vpc-id,Values=$vid" --output json 2>/dev/null || echo '{"Subnets":[]}')
                while IFS= read -r sid; do
                    [[ -z "$sid" ]] && continue
                    resources=$(echo "$resources" | jq --arg id "$sid" \
                        '. += [{"LogicalResourceId":"Subnet","PhysicalResourceId":$id,"ResourceType":"AWS::EC2::Subnet","ResourceStatus":"DISCOVERED"}]')
                done < <(echo "$subnets" | jq -r '.Subnets[].SubnetId' 2>/dev/null)

                # Security Groups (non-default)
                local sgs
                sgs=$(aws_cmd ec2 describe-security-groups --filters "Name=vpc-id,Values=$vid" --output json 2>/dev/null || echo '{"SecurityGroups":[]}')
                while IFS= read -r sg_id; do
                    [[ -z "$sg_id" ]] && continue
                    resources=$(echo "$resources" | jq --arg id "$sg_id" \
                        '. += [{"LogicalResourceId":"SecurityGroup","PhysicalResourceId":$id,"ResourceType":"AWS::EC2::SecurityGroup","ResourceStatus":"DISCOVERED"}]')
                done < <(echo "$sgs" | jq -r '.SecurityGroups[] | select(.GroupName != "default") | .GroupId' 2>/dev/null)

                # NAT Gateways
                local nats
                nats=$(aws_cmd ec2 describe-nat-gateways --filter "Name=vpc-id,Values=$vid" "Name=state,Values=available" --output json 2>/dev/null || echo '{"NatGateways":[]}')
                while IFS= read -r nid; do
                    [[ -z "$nid" ]] && continue
                    resources=$(echo "$resources" | jq --arg id "$nid" \
                        '. += [{"LogicalResourceId":"NATGW","PhysicalResourceId":$id,"ResourceType":"AWS::EC2::NatGateway","ResourceStatus":"DISCOVERED"}]')
                done < <(echo "$nats" | jq -r '.NatGateways[].NatGatewayId' 2>/dev/null)

                # Internet Gateways
                local igws
                igws=$(aws_cmd ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$vid" --output json 2>/dev/null || echo '{"InternetGateways":[]}')
                while IFS= read -r igid; do
                    [[ -z "$igid" ]] && continue
                    resources=$(echo "$resources" | jq --arg id "$igid" \
                        '. += [{"LogicalResourceId":"IGW","PhysicalResourceId":$id,"ResourceType":"AWS::EC2::InternetGateway","ResourceStatus":"DISCOVERED"}]')
                done < <(echo "$igws" | jq -r '.InternetGateways[].InternetGatewayId' 2>/dev/null)

                # VPC Endpoints
                local vpces
                vpces=$(aws_cmd ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=$vid" --output json 2>/dev/null || echo '{"VpcEndpoints":[]}')
                while IFS= read -r eid; do
                    [[ -z "$eid" ]] && continue
                    resources=$(echo "$resources" | jq --arg id "$eid" \
                        '. += [{"LogicalResourceId":"VpcEndpoint","PhysicalResourceId":$id,"ResourceType":"AWS::EC2::VPCEndpoint","ResourceStatus":"DISCOVERED"}]')
                done < <(echo "$vpces" | jq -r '.VpcEndpoints[].VpcEndpointId' 2>/dev/null)

            done < <(echo "$result" | jq -r "$jq_extract" 2>/dev/null)
        fi

    done <<< "$enabled_types"

    # ─── Build synthetic STACK_RESOURCES ───
    STACK_RESOURCES=$(echo "$resources" | jq '{StackResourceSummaries: .}')

    local total
    total=$(echo "$resources" | jq 'length')

    if [[ "$JSON_OUTPUT" != "true" ]]; then
        printf "    %-50s\r" ""
        echo ""
        echo -e "    ${GREEN}✓${RESET} Discovery complete: ${BOLD}${total} resources${RESET} found"
        echo ""
        if [[ "$total" -gt 0 ]]; then
            print_subheader "Discovered Resources Summary"
            echo ""
            echo "$resources" | jq -r 'group_by(.ResourceType) | .[] | "\(.[0].ResourceType)|\(length)"' 2>/dev/null | sort | \
            while IFS='|' read -r rtype rcount; do
                printf "    %-55s %s\n" "$rtype" "${rcount} found"
            done
            echo ""
        fi
    fi

    # Set stack name for display purposes
    STACK_NAME="[MANUAL SCAN]"
}


###############################################################################
# CLOUDFORMATION STACK AUDIT
###############################################################################

audit_cloudformation() {
    print_header "CLOUDFORMATION STACK"

    local stack_info
    stack_info=$(aws_cmd cloudformation describe-stacks --stack-name "$STACK_NAME" --output json 2>/dev/null || echo "")

    if [[ -z "$stack_info" || "$stack_info" == "" ]]; then
        print_status "Stack" "NOT FOUND" "error"
        ((RESOURCES_UNHEALTHY++)) || true
        return
    fi

    local stack=$(echo "$stack_info" | jq '.Stacks[0]')
    local status=$(json_get "$stack" '.StackStatus')
    local created=$(json_get "$stack" '.CreationTime')
    local updated=$(json_get "$stack" '.LastUpdatedTime')
    local description=$(json_get "$stack" '.Description')
    local stack_id=$(json_get "$stack" '.StackId')

    ((RESOURCES_FOUND++)) || true
    ((RESOURCES_EXPECTED++)) || true

    if [[ "$status" == *"COMPLETE"* && "$status" != *"DELETE"* ]]; then
        ((RESOURCES_HEALTHY++)) || true
        print_status "Stack Name" "$STACK_NAME" "ok"
    else
        ((RESOURCES_UNHEALTHY++)) || true
        print_status "Stack Name" "$STACK_NAME" "error"
    fi

    print_status "Status" "$(echo -e "$(status_color "$status")")" "info"
    print_status "Created" "$created" "info"
    [[ -n "$updated" ]] && print_status "Last Updated" "$updated" "info"
    [[ -n "$description" ]] && print_status "Description" "$description" "info"

    # Stack outputs
    print_subheader "Stack Outputs"
    local outputs
    outputs=$(echo "$stack" | jq -r '.Outputs[]? | "\(.OutputKey)|\(.OutputValue)"' 2>/dev/null || echo "")
    
    if [[ -n "$outputs" ]]; then
        print_table_header "Output Key" "Value"
        while IFS='|' read -r key value; do
            # Truncate long values
            if [[ ${#value} -gt 60 ]]; then
                value="${value:0:57}..."
            fi
            print_table_row "$key" "$value"
        done <<< "$outputs"
    else
        print_status "Outputs" "None" "warn"
    fi

    # Stack parameters
    print_subheader "Stack Parameters"
    local params
    params=$(echo "$stack" | jq -r '.Parameters[]? | "\(.ParameterKey)|\(.ParameterValue)"' 2>/dev/null || echo "")
    
    if [[ -n "$params" ]]; then
        print_table_header "Parameter" "Value"
        while IFS='|' read -r key value; do
            if [[ ${#value} -gt 60 ]]; then
                value="${value:0:57}..."
            fi
            print_table_row "$key" "$value"
        done <<< "$params"
    fi

    # Detect stack drift
    print_subheader "Drift Detection"
    local drift_status
    drift_status=$(aws_cmd cloudformation detect-stack-drift --stack-name "$STACK_NAME" --output json 2>/dev/null || echo "")
    
    if [[ -n "$drift_status" ]]; then
        local drift_id=$(json_get "$drift_status" '.StackDriftDetectionId')
        print_status "Drift Detection" "Initiated (ID: ${drift_id:0:20}...)" "info"
        
        # Wait briefly for drift results
        sleep 2
        local drift_result
        drift_result=$(aws_cmd cloudformation describe-stack-drift-detection-status \
            --stack-drift-detection-id "$drift_id" --output json 2>/dev/null || echo "")
        
        if [[ -n "$drift_result" ]]; then
            local drift_stat=$(json_get "$drift_result" '.StackDriftStatus')
            local detection_status=$(json_get "$drift_result" '.DetectionStatus')
            
            if [[ "$detection_status" == "DETECTION_COMPLETE" ]]; then
                case "$drift_stat" in
                    IN_SYNC)     print_status "Drift Status" "IN SYNC" "ok" ;;
                    DRIFTED)     print_status "Drift Status" "DRIFTED" "warn" ;;
                    *)           print_status "Drift Status" "$drift_stat" "info" ;;
                esac
            else
                print_status "Drift Status" "Detection in progress ($detection_status)" "info"
            fi
        fi
    else
        print_status "Drift Detection" "Unable to initiate" "warn"
    fi

    # Store for JSON
    JSON_SECTION_CLOUDFORMATION=$(cat <<EOF
{
    "stack_name": "$STACK_NAME",
    "status": "$status",
    "created": "$created",
    "last_updated": "$updated",
    "outputs": $(echo "$stack" | jq '.Outputs // []'),
    "parameters": $(echo "$stack" | jq '.Parameters // []')
}
EOF
)

    # Get resource list for other sections
    STACK_RESOURCES=$(aws_cmd cloudformation list-stack-resources --stack-name "$STACK_NAME" --output json 2>/dev/null || echo "{}")
}


###############################################################################
# VPC & NETWORKING AUDIT
###############################################################################

audit_networking() {
    print_header "VPC & NETWORKING"

    local vpc_id=""
    local vpc_json="[]"

    # Try to get VPC from stack outputs or resources
    vpc_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="VPC") | .PhysicalResourceId' 2>/dev/null || echo "")

    # VPC
    print_subheader "VPC"
    ((RESOURCES_EXPECTED++)) || true
    if [[ -n "$vpc_id" ]]; then
        ((RESOURCES_FOUND++)) || true
        local vpc_info
        vpc_info=$(aws_cmd ec2 describe-vpcs --vpc-ids "$vpc_id" --output json 2>/dev/null || echo "")
        
        if [[ -n "$vpc_info" && $(echo "$vpc_info" | jq '.Vpcs | length') -gt 0 ]]; then
            ((RESOURCES_HEALTHY++)) || true
            local cidr=$(echo "$vpc_info" | jq -r '.Vpcs[0].CidrBlock')
            local state=$(echo "$vpc_info" | jq -r '.Vpcs[0].State')
            local name=$(echo "$vpc_info" | jq -r '.Vpcs[0].Tags[]? | select(.Key=="Name") | .Value // "unnamed"' 2>/dev/null || echo "unnamed")
            
            print_status "VPC ID" "$vpc_id" "ok"
            print_status "State" "$state" "ok"
            print_status "CIDR Block" "$cidr" "info"
            print_status "Name" "$name" "info"
        else
            ((RESOURCES_UNHEALTHY++)) || true
            print_status "VPC" "$vpc_id (not accessible)" "error"
        fi
    else
        print_status "VPC" "Not created by stack (existing-vpc template or analytics-only)" "info"
    fi

    # Subnets
    print_subheader "Subnets"
    local subnet_ids=()
    while IFS= read -r sid; do
        [[ -n "$sid" ]] && subnet_ids+=("$sid")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId | startswith("Subnet")) | .PhysicalResourceId' 2>/dev/null)

    if [[ ${#subnet_ids[@]} -gt 0 ]]; then
        print_table_header "Subnet ID" "AZ" "CIDR" "Available IPs"
        for subnet_id in "${subnet_ids[@]}"; do
            ((RESOURCES_EXPECTED++)) || true
            ((RESOURCES_FOUND++)) || true
            local subnet_info
            subnet_info=$(aws_cmd ec2 describe-subnets --subnet-ids "$subnet_id" --output json 2>/dev/null || echo "")
            if [[ -n "$subnet_info" && $(echo "$subnet_info" | jq '.Subnets | length') -gt 0 ]]; then
                ((RESOURCES_HEALTHY++)) || true
                local az=$(echo "$subnet_info" | jq -r '.Subnets[0].AvailabilityZone')
                local cidr=$(echo "$subnet_info" | jq -r '.Subnets[0].CidrBlock')
                local avail_ips=$(echo "$subnet_info" | jq -r '.Subnets[0].AvailableIpAddressCount')
                print_table_row "$subnet_id" "$az" "$cidr" "$avail_ips"
            fi
        done
    else
        print_status "Subnets" "Not created by this stack" "info"
    fi

    # Security Groups
    print_subheader "Security Groups"
    local sg_ids=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && sg_ids+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType=="AWS::EC2::SecurityGroup") | "\(.LogicalResourceId)|\(.PhysicalResourceId)"' 2>/dev/null)

    if [[ ${#sg_ids[@]} -gt 0 ]]; then
        print_table_header "Logical ID" "Group ID" "Ingress Rules"
        for sg_entry in "${sg_ids[@]}"; do
            IFS='|' read -r logical_id sg_id <<< "$sg_entry"
            ((RESOURCES_EXPECTED++)) || true
            ((RESOURCES_FOUND++)) || true
            local sg_info
            sg_info=$(aws_cmd ec2 describe-security-groups --group-ids "$sg_id" --output json 2>/dev/null || echo "")
            if [[ -n "$sg_info" && $(echo "$sg_info" | jq '.SecurityGroups | length') -gt 0 ]]; then
                ((RESOURCES_HEALTHY++)) || true
                local rule_count=$(echo "$sg_info" | jq '.SecurityGroups[0].IpPermissions | length')
                print_table_row "$logical_id" "$sg_id" "$rule_count rules"
            fi
        done
    else
        print_status "Security Groups" "None found in stack" "info"
    fi

    # Internet Gateway
    print_subheader "Internet & NAT Gateways"
    local igw_id
    igw_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="IGW") | .PhysicalResourceId' 2>/dev/null || echo "")
    
    if [[ -n "$igw_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "Internet Gateway" "$igw_id" "ok"
    else
        print_status "Internet Gateway" "Not in stack" "info"
    fi

    # NAT Gateway
    local nat_id
    nat_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NATGW") | .PhysicalResourceId' 2>/dev/null || echo "")
    
    if [[ -n "$nat_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        local nat_info
        nat_info=$(aws_cmd ec2 describe-nat-gateways --nat-gateway-ids "$nat_id" --output json 2>/dev/null || echo "")
        if [[ -n "$nat_info" ]]; then
            local nat_state=$(echo "$nat_info" | jq -r '.NatGateways[0].State')
            if [[ "$nat_state" == "available" ]]; then
                ((RESOURCES_HEALTHY++)) || true
                print_status "NAT Gateway" "$nat_id ($nat_state)" "ok"
            else
                ((RESOURCES_UNHEALTHY++)) || true
                print_status "NAT Gateway" "$nat_id ($nat_state)" "warn"
            fi
            print_status "  Cost Note" "NAT Gateway: ~\$0.045/hr + data processing" "warn"
        fi
    else
        print_status "NAT Gateway" "Not in stack" "info"
    fi

    # Elastic IP
    local eip_id
    eip_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="ElasticIP1") | .PhysicalResourceId' 2>/dev/null || echo "")
    
    if [[ -n "$eip_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "Elastic IP" "$eip_id" "ok"
        print_status "  Cost Note" "EIP: \$3.65/month if unattached" "info"
    fi

    # VPC Endpoints
    print_subheader "VPC Endpoints"
    local vpce_ids=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && vpce_ids+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType | contains("VpcEndpoint")) | "\(.LogicalResourceId)|\(.PhysicalResourceId)"' 2>/dev/null)

    if [[ ${#vpce_ids[@]} -gt 0 ]]; then
        for vpce_entry in "${vpce_ids[@]}"; do
            IFS='|' read -r logical_id vpce_id <<< "$vpce_entry"
            ((RESOURCES_EXPECTED++)) || true
            ((RESOURCES_FOUND++)) || true
            ((RESOURCES_HEALTHY++)) || true
            print_status "$logical_id" "$vpce_id" "ok"
        done
    else
        print_status "VPC Endpoints" "None in stack" "info"
    fi
}


###############################################################################
# NEPTUNE DB AUDIT
###############################################################################

audit_neptune_db() {
    local cluster_id
    cluster_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneDBCluster") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -z "$cluster_id" ]]; then
        return
    fi

    print_header "NEPTUNE DB CLUSTER"
    ((RESOURCES_EXPECTED++)) || true

    local cluster_info
    cluster_info=$(aws_cmd neptune describe-db-clusters --db-cluster-identifier "$cluster_id" --output json 2>/dev/null || echo "")

    if [[ -z "$cluster_info" || $(echo "$cluster_info" | jq '.DBClusters | length') -eq 0 ]]; then
        print_status "Neptune Cluster" "$cluster_id - NOT FOUND" "error"
        ((RESOURCES_UNHEALTHY++)) || true
        return
    fi

    ((RESOURCES_FOUND++)) || true
    local cluster=$(echo "$cluster_info" | jq '.DBClusters[0]')
    local status=$(json_get "$cluster" '.Status')
    local endpoint=$(json_get "$cluster" '.Endpoint')
    local reader_endpoint=$(json_get "$cluster" '.ReaderEndpoint')
    local engine_version=$(json_get "$cluster" '.EngineVersion')
    local port=$(json_get "$cluster" '.Port')
    local storage=$(json_get "$cluster" '.AllocatedStorage')
    local serverless_config=$(echo "$cluster" | jq '.ServerlessV2ScalingConfiguration // empty' 2>/dev/null)

    if [[ "$status" == "available" ]]; then
        ((RESOURCES_HEALTHY++)) || true
        print_status "Cluster ID" "$cluster_id" "ok"
    else
        ((RESOURCES_UNHEALTHY++)) || true
        print_status "Cluster ID" "$cluster_id" "warn"
    fi

    print_status "Status" "$(echo -e "$(status_color "$status")")" "info"
    print_status "Endpoint" "$endpoint" "info"
    print_status "Reader Endpoint" "$reader_endpoint" "info"
    print_status "Engine Version" "$engine_version" "info"
    print_status "Port" "$port" "info"
    
    if [[ -n "$serverless_config" ]]; then
        local min_ncu=$(json_get "$serverless_config" '.MinCapacity')
        local max_ncu=$(json_get "$serverless_config" '.MaxCapacity')
        print_status "Serverless Mode" "Min: ${min_ncu} NCU / Max: ${max_ncu} NCU" "info"
        print_status "  Cost Note" "Neptune Serverless: \$0.1028/NCU-hour" "warn"
    fi

    # Neptune DB Instances
    print_subheader "Neptune DB Instances"
    local instance_id
    instance_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneDBInstance") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -n "$instance_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        local inst_info
        inst_info=$(aws_cmd neptune describe-db-instances --db-instance-identifier "$instance_id" --output json 2>/dev/null || echo "")
        
        if [[ -n "$inst_info" && $(echo "$inst_info" | jq '.DBInstances | length') -gt 0 ]]; then
            ((RESOURCES_FOUND++)) || true
            local inst=$(echo "$inst_info" | jq '.DBInstances[0]')
            local inst_status=$(json_get "$inst" '.DBInstanceStatus')
            local inst_class=$(json_get "$inst" '.DBInstanceClass')
            local inst_az=$(json_get "$inst" '.AvailabilityZone')

            if [[ "$inst_status" == "available" ]]; then
                ((RESOURCES_HEALTHY++)) || true
            else
                ((RESOURCES_UNHEALTHY++)) || true
            fi

            print_status "Instance ID" "$instance_id" "ok"
            print_status "Status" "$(echo -e "$(status_color "$inst_status")")" "info"
            print_status "Instance Class" "$inst_class" "info"
            print_status "AZ" "$inst_az" "info"
            
            if [[ "$inst_class" == "db.serverless" ]]; then
                print_status "  Cost Note" "Serverless - pay per NCU-hour" "info"
            else
                print_status "  Cost Note" "Instance $inst_class running 24/7" "warn"
            fi
        fi
    fi

    # Parameter Groups
    local param_group
    param_group=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneDBClusterParameterGroup") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$param_group" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "Cluster Param Group" "$param_group" "ok"
    fi

    local db_param_group
    db_param_group=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneDBParameterGroup") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$db_param_group" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "DB Param Group" "$db_param_group" "ok"
    fi

    # Subnet Group
    local subnet_group
    subnet_group=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneDBSubnetGroup") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$subnet_group" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "Subnet Group" "$subnet_group" "ok"
    fi
}

###############################################################################
# NEPTUNE ANALYTICS AUDIT
###############################################################################

audit_neptune_analytics() {
    local graph_id
    graph_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="Graph") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -z "$graph_id" ]]; then
        return
    fi

    print_header "NEPTUNE ANALYTICS GRAPH"
    ((RESOURCES_EXPECTED++)) || true

    local graph_info
    graph_info=$(aws_cmd neptune-graph get-graph --graph-identifier "$graph_id" --output json 2>/dev/null || echo "")

    if [[ -z "$graph_info" ]]; then
        print_status "Neptune Graph" "$graph_id - NOT FOUND" "error"
        ((RESOURCES_UNHEALTHY++)) || true
        return
    fi

    ((RESOURCES_FOUND++)) || true
    local status=$(json_get "$graph_info" '.status')
    local name=$(json_get "$graph_info" '.name')
    local memory=$(json_get "$graph_info" '.provisionedMemory')
    local endpoint=$(json_get "$graph_info" '.endpoint')
    local vector_search=$(json_get "$graph_info" '.vectorSearchConfiguration.dimension')
    local public_access=$(json_get "$graph_info" '.publicConnectivity')

    if [[ "$status" == "AVAILABLE" ]]; then
        ((RESOURCES_HEALTHY++)) || true
        print_status "Graph Name" "$name" "ok"
    else
        ((RESOURCES_UNHEALTHY++)) || true
        print_status "Graph Name" "$name" "warn"
    fi

    print_status "Graph ID" "$graph_id" "info"
    print_status "Status" "$(echo -e "$(status_color "$status")")" "info"
    print_status "Provisioned Memory" "${memory} GB" "info"
    print_status "Endpoint" "$endpoint" "info"
    [[ -n "$vector_search" ]] && print_status "Vector Dimensions" "$vector_search" "info"
    print_status "Public Access" "$public_access" "info"
    print_status "  Cost Note" "Neptune Analytics: ~\$0.11/GB-hour (${memory}GB = ~\$$(echo "$memory * 0.11" | bc 2>/dev/null || echo "N/A")/hr)" "warn"
}


###############################################################################
# OPENSEARCH SERVERLESS AUDIT
###############################################################################

audit_opensearch() {
    local collection_id
    collection_id=$(echo "$STACK_RESOURCES" | jq -r '[.StackResourceSummaries[]? | select(.LogicalResourceId=="OpenSearchServerless") | .PhysicalResourceId] | first // empty' 2>/dev/null || echo "")

    if [[ -z "$collection_id" ]]; then
        return
    fi

    print_header "OPENSEARCH SERVERLESS"

    # In manual mode, there may be multiple collections — audit each one
    local all_collection_ids=()
    while IFS= read -r cid; do
        [[ -n "$cid" ]] && all_collection_ids+=("$cid")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="OpenSearchServerless") | .PhysicalResourceId' 2>/dev/null)

    for collection_id in "${all_collection_ids[@]+"${all_collection_ids[@]}"}"; do
        [[ -z "$collection_id" ]] && continue
        ((RESOURCES_EXPECTED++)) || true

        # Get collection details
        local collection_info
        collection_info=$(aws_cmd opensearchserverless batch-get-collection --ids "$collection_id" --output json 2>/dev/null || echo "")

        if [[ -z "$collection_info" || $(echo "$collection_info" | jq '.collectionDetails | length') -eq 0 ]]; then
            print_status "Collection" "$collection_id - NOT FOUND" "error"
            ((RESOURCES_UNHEALTHY++)) || true
            continue
        fi

        ((RESOURCES_FOUND++)) || true
        local collection=$(echo "$collection_info" | jq '.collectionDetails[0]')
        local status=$(json_get "$collection" '.status')
        local name=$(json_get "$collection" '.name')
        local coll_type=$(json_get "$collection" '.type')
        local endpoint=$(json_get "$collection" '.collectionEndpoint')
        local dashboard=$(json_get "$collection" '.dashboardEndpoint')
        local arn=$(json_get "$collection" '.arn')

        if [[ "$status" == "ACTIVE" ]]; then
            ((RESOURCES_HEALTHY++)) || true
            print_status "Collection Name" "$name" "ok"
        else
            ((RESOURCES_UNHEALTHY++)) || true
            print_status "Collection Name" "$name" "warn"
        fi

        print_status "Collection ID" "$collection_id" "info"
        print_status "Status" "$(echo -e "$(status_color "$status")")" "info"
        print_status "Type" "$coll_type" "info"
        print_status "Endpoint" "$endpoint" "info"
        print_status "Dashboard" "$dashboard" "info"
        print_status "  Cost Note" "OpenSearch Serverless: ~\$0.24/OCU-hour (min 2 OCUs indexing + 2 OCUs search)" "warn"
        echo ""
    done

    # Security Policies
    print_subheader "Security & Access Policies"
    local policy_ids=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && policy_ids+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType | contains("OpenSearchServerless")) | select(.LogicalResourceId != "OpenSearchServerless") | "\(.LogicalResourceId)|\(.PhysicalResourceId)"' 2>/dev/null)

    for policy_entry in "${policy_ids[@]+"${policy_ids[@]}"}"; do
        IFS='|' read -r logical_id phys_id <<< "$policy_entry"
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "$logical_id" "${phys_id:0:50}" "ok"
    done

    # VPC Endpoint for OpenSearch
    local vpce_id
    vpce_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="OpenSearchServerlessVpcEndpoint") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$vpce_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "VPC Endpoint" "$vpce_id" "ok"
    fi
}

###############################################################################
# AURORA POSTGRESQL AUDIT
###############################################################################

audit_aurora_postgres() {
    local cluster_id
    cluster_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="PostgresCluster") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -z "$cluster_id" ]]; then
        return
    fi

    print_header "AURORA POSTGRESQL"
    ((RESOURCES_EXPECTED++)) || true

    local cluster_info
    cluster_info=$(aws_cmd rds describe-db-clusters --db-cluster-identifier "$cluster_id" --output json 2>/dev/null || echo "")

    if [[ -z "$cluster_info" || $(echo "$cluster_info" | jq '.DBClusters | length') -eq 0 ]]; then
        print_status "Aurora Cluster" "$cluster_id - NOT FOUND" "error"
        ((RESOURCES_UNHEALTHY++)) || true
        return
    fi

    ((RESOURCES_FOUND++)) || true
    local cluster=$(echo "$cluster_info" | jq '.DBClusters[0]')
    local status=$(json_get "$cluster" '.Status')
    local endpoint=$(json_get "$cluster" '.Endpoint')
    local reader_endpoint=$(json_get "$cluster" '.ReaderEndpoint')
    local engine=$(json_get "$cluster" '.Engine')
    local engine_version=$(json_get "$cluster" '.EngineVersion')
    local port=$(json_get "$cluster" '.Port')
    local serverless_config=$(echo "$cluster" | jq '.ServerlessV2ScalingConfiguration // empty' 2>/dev/null)

    if [[ "$status" == "available" ]]; then
        ((RESOURCES_HEALTHY++)) || true
        print_status "Cluster ID" "$cluster_id" "ok"
    else
        ((RESOURCES_UNHEALTHY++)) || true
        print_status "Cluster ID" "$cluster_id" "warn"
    fi

    print_status "Status" "$(echo -e "$(status_color "$status")")" "info"
    print_status "Engine" "$engine $engine_version" "info"
    print_status "Endpoint" "$endpoint" "info"
    print_status "Reader Endpoint" "$reader_endpoint" "info"
    print_status "Port" "$port" "info"

    if [[ -n "$serverless_config" ]]; then
        local min_acu=$(json_get "$serverless_config" '.MinCapacity')
        local max_acu=$(json_get "$serverless_config" '.MaxCapacity')
        print_status "Serverless Mode" "Min: ${min_acu} ACU / Max: ${max_acu} ACU" "info"
        print_status "  Cost Note" "Aurora Serverless v2: \$0.12/ACU-hour" "warn"
    fi

    # Aurora Instances
    print_subheader "Aurora DB Instances"
    local instance_id
    instance_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="PostgresInstance") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -n "$instance_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        local inst_info
        inst_info=$(aws_cmd rds describe-db-instances --db-instance-identifier "$instance_id" --output json 2>/dev/null || echo "")

        if [[ -n "$inst_info" && $(echo "$inst_info" | jq '.DBInstances | length') -gt 0 ]]; then
            ((RESOURCES_FOUND++)) || true
            local inst=$(echo "$inst_info" | jq '.DBInstances[0]')
            local inst_status=$(json_get "$inst" '.DBInstanceStatus')
            local inst_class=$(json_get "$inst" '.DBInstanceClass')
            local inst_az=$(json_get "$inst" '.AvailabilityZone')
            local storage_type=$(json_get "$inst" '.StorageType')

            if [[ "$inst_status" == "available" ]]; then
                ((RESOURCES_HEALTHY++)) || true
            else
                ((RESOURCES_UNHEALTHY++)) || true
            fi

            print_status "Instance ID" "$instance_id" "ok"
            print_status "Status" "$(echo -e "$(status_color "$inst_status")")" "info"
            print_status "Instance Class" "$inst_class" "info"
            print_status "AZ" "$inst_az" "info"
            print_status "Storage Type" "$storage_type" "info"
        fi
    fi

    # Parameter Groups and Subnet Group
    local pg_cluster
    pg_cluster=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="PostgresClusterParameterGroup") | .PhysicalResourceId' 2>/dev/null || echo "")
    [[ -n "$pg_cluster" ]] && { ((RESOURCES_EXPECTED++)); ((RESOURCES_FOUND++)); ((RESOURCES_HEALTHY++)); print_status "Cluster Param Group" "$pg_cluster" "ok"; } || true

    local pg_db
    pg_db=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="PostgresDBParameterGroup") | .PhysicalResourceId' 2>/dev/null || echo "")
    [[ -n "$pg_db" ]] && { ((RESOURCES_EXPECTED++)); ((RESOURCES_FOUND++)); ((RESOURCES_HEALTHY++)); print_status "DB Param Group" "$pg_db" "ok"; } || true

    local pg_subnet
    pg_subnet=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="PostgresSubnetGroup") | .PhysicalResourceId' 2>/dev/null || echo "")
    [[ -n "$pg_subnet" ]] && { ((RESOURCES_EXPECTED++)); ((RESOURCES_FOUND++)); ((RESOURCES_HEALTHY++)); print_status "Subnet Group" "$pg_subnet" "ok"; } || true
}

###############################################################################
# S3 VECTORS AUDIT
###############################################################################

audit_s3_vectors() {
    local bucket_id
    bucket_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="VectorBucket") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -z "$bucket_id" ]]; then
        return
    fi

    print_header "S3 VECTORS"
    ((RESOURCES_EXPECTED++)) || true

    # S3 Vectors is a newer service; check via the resource status
    local resource_status
    resource_status=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="VectorBucket") | .ResourceStatus' 2>/dev/null || echo "")

    if [[ "$resource_status" == "CREATE_COMPLETE" || "$resource_status" == "UPDATE_COMPLETE" ]]; then
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "Vector Bucket" "$bucket_id" "ok"
        print_status "Resource Status" "$resource_status" "ok"
    else
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_UNHEALTHY++)) || true
        print_status "Vector Bucket" "$bucket_id ($resource_status)" "warn"
    fi

    # KMS Key
    print_subheader "KMS Encryption"
    local kms_key
    kms_key=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="VectorBucketKMSKey") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -n "$kms_key" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true

        local key_info
        key_info=$(aws_cmd kms describe-key --key-id "$kms_key" --output json 2>/dev/null || echo "")

        if [[ -n "$key_info" ]]; then
            local key_state=$(echo "$key_info" | jq -r '.KeyMetadata.KeyState')
            local key_spec=$(echo "$key_info" | jq -r '.KeyMetadata.KeySpec')
            
            if [[ "$key_state" == "Enabled" ]]; then
                ((RESOURCES_HEALTHY++)) || true
                print_status "KMS Key" "$kms_key" "ok"
            else
                ((RESOURCES_UNHEALTHY++)) || true
                print_status "KMS Key" "$kms_key ($key_state)" "warn"
            fi
            print_status "Key State" "$key_state" "info"
            print_status "Key Spec" "$key_spec" "info"
            print_status "  Cost Note" "KMS: \$1/month/key + \$0.03/10K requests" "info"
        fi
    fi
}


###############################################################################
# SAGEMAKER NOTEBOOK AUDIT
###############################################################################

audit_sagemaker() {
    local notebook_id
    notebook_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneNotebookInstance") | .PhysicalResourceId' 2>/dev/null || echo "")

    if [[ -z "$notebook_id" ]]; then
        return
    fi

    print_header "SAGEMAKER NOTEBOOK"
    ((RESOURCES_EXPECTED++)) || true

    local notebook_info
    notebook_info=$(aws_cmd sagemaker describe-notebook-instance --notebook-instance-name "$notebook_id" --output json 2>/dev/null || echo "")

    if [[ -z "$notebook_info" ]]; then
        print_status "Notebook" "$notebook_id - NOT FOUND" "error"
        ((RESOURCES_UNHEALTHY++)) || true
        return
    fi

    ((RESOURCES_FOUND++)) || true
    local status=$(json_get "$notebook_info" '.NotebookInstanceStatus')
    local instance_type=$(json_get "$notebook_info" '.InstanceType')
    local url=$(json_get "$notebook_info" '.Url')
    local volume_size=$(json_get "$notebook_info" '.VolumeSizeInGB')
    local direct_access=$(json_get "$notebook_info" '.DirectInternetAccess')
    local role_arn=$(json_get "$notebook_info" '.RoleArn')

    if [[ "$status" == "InService" ]]; then
        ((RESOURCES_HEALTHY++)) || true
        print_status "Notebook Name" "$notebook_id" "ok"
        print_status "  Cost Note" "RUNNING - ${instance_type} incurring charges" "warn"
    elif [[ "$status" == "Stopped" ]]; then
        ((RESOURCES_HEALTHY++)) || true
        print_status "Notebook Name" "$notebook_id" "ok"
        print_status "  Cost Note" "Stopped - only storage charges (\$${volume_size:-5}GB EBS)" "info"
    else
        ((RESOURCES_UNHEALTHY++)) || true
        print_status "Notebook Name" "$notebook_id" "warn"
    fi

    print_status "Status" "$(echo -e "$(status_color "$status")")" "info"
    print_status "Instance Type" "$instance_type" "info"
    print_status "Volume Size" "${volume_size}GB" "info"
    print_status "URL" "$url" "info"
    print_status "Direct Internet" "$direct_access" "info"
    print_status "IAM Role" "$(echo "$role_arn" | awk -F'/' '{print $NF}')" "info"

    # Lifecycle Config
    local lifecycle_id
    lifecycle_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneNotebookInstanceLifecycleConfig") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$lifecycle_id" ]]; then
        ((RESOURCES_EXPECTED++)) || true
        ((RESOURCES_FOUND++)) || true
        ((RESOURCES_HEALTHY++)) || true
        print_status "Lifecycle Config" "$lifecycle_id" "ok"
    fi
}

###############################################################################
# IAM ROLES & POLICIES AUDIT
###############################################################################

audit_iam() {
    print_header "IAM ROLES & POLICIES"

    # Roles
    print_subheader "IAM Roles"
    local roles=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && roles+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType=="AWS::IAM::Role") | "\(.LogicalResourceId)|\(.PhysicalResourceId)"' 2>/dev/null)

    if [[ ${#roles[@]} -gt 0 ]]; then
        print_table_header "Logical ID" "Role Name" "Status"
        for role_entry in "${roles[@]}"; do
            IFS='|' read -r logical_id role_name <<< "$role_entry"
            ((RESOURCES_EXPECTED++)) || true

            local role_info
            role_info=$(aws_cmd iam get-role --role-name "$role_name" --output json 2>/dev/null || echo "")

            if [[ -n "$role_info" ]]; then
                ((RESOURCES_FOUND++)) || true
                ((RESOURCES_HEALTHY++)) || true
                local create_date=$(echo "$role_info" | jq -r '.Role.CreateDate' 2>/dev/null)
                print_table_row "$logical_id" "${role_name:0:40}" "${GREEN}EXISTS${RESET}"
            else
                ((RESOURCES_UNHEALTHY++)) || true
                print_table_row "$logical_id" "${role_name:0:40}" "${RED}NOT FOUND${RESET}"
            fi
        done
    else
        print_status "IAM Roles" "None found in stack" "info"
    fi

    # Managed Policies
    print_subheader "IAM Managed Policies"
    local policies=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && policies+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType=="AWS::IAM::ManagedPolicy") | "\(.LogicalResourceId)|\(.PhysicalResourceId)"' 2>/dev/null)

    if [[ ${#policies[@]} -gt 0 ]]; then
        print_table_header "Policy Name" "ARN (truncated)"
        for policy_entry in "${policies[@]}"; do
            IFS='|' read -r logical_id policy_arn <<< "$policy_entry"
            ((RESOURCES_EXPECTED++)) || true
            ((RESOURCES_FOUND++)) || true
            ((RESOURCES_HEALTHY++)) || true
            
            # Truncate ARN for display
            local display_arn="${policy_arn}"
            if [[ ${#display_arn} -gt 70 ]]; then
                display_arn="...${display_arn: -67}"
            fi
            print_table_row "$logical_id" "$display_arn"
        done
    else
        print_status "Managed Policies" "None found in stack" "info"
    fi
}

###############################################################################
# LAMBDA FUNCTIONS AUDIT
###############################################################################

audit_lambda() {
    local lambdas=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && lambdas+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType=="AWS::Lambda::Function") | "\(.LogicalResourceId)|\(.PhysicalResourceId)"' 2>/dev/null)

    if [[ ${#lambdas[@]} -eq 0 ]]; then
        return
    fi

    print_header "LAMBDA FUNCTIONS"

    for lambda_entry in "${lambdas[@]}"; do
        IFS='|' read -r logical_id function_name <<< "$lambda_entry"
        ((RESOURCES_EXPECTED++)) || true

        local func_info
        func_info=$(aws_cmd lambda get-function --function-name "$function_name" --output json 2>/dev/null || echo "")

        if [[ -z "$func_info" ]]; then
            ((RESOURCES_UNHEALTHY++)) || true
            print_status "$logical_id" "$function_name - NOT FOUND" "error"
            continue
        fi

        ((RESOURCES_FOUND++)) || true
        local config=$(echo "$func_info" | jq '.Configuration')
        local state=$(json_get "$config" '.State')
        local runtime=$(json_get "$config" '.Runtime')
        local memory=$(json_get "$config" '.MemorySize')
        local timeout=$(json_get "$config" '.Timeout')
        local last_modified=$(json_get "$config" '.LastModified')
        local code_size=$(json_get "$config" '.CodeSize')

        if [[ "$state" == "Active" ]]; then
            ((RESOURCES_HEALTHY++)) || true
            print_status "$logical_id" "$function_name" "ok"
        else
            ((RESOURCES_UNHEALTHY++)) || true
            print_status "$logical_id" "$function_name ($state)" "warn"
        fi

        print_status "  Runtime" "$runtime" "info"
        print_status "  Memory" "${memory}MB / Timeout: ${timeout}s" "info"
        print_status "  Code Size" "$(echo "$code_size" | awk '{printf "%.1f KB", $1/1024}')" "info"
        print_status "  Last Modified" "$last_modified" "info"
    done
}


###############################################################################
# ADDITIONAL RESOURCES AUDIT (Route Tables, Custom Resources)
###############################################################################

audit_additional_resources() {
    # Check for any resources we haven't explicitly covered
    local other_resources=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && other_resources+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(
        .ResourceType != "AWS::EC2::VPC" and
        .ResourceType != "AWS::EC2::Subnet" and
        .ResourceType != "AWS::EC2::InternetGateway" and
        .ResourceType != "AWS::EC2::VPCGatewayAttachment" and
        .ResourceType != "AWS::EC2::NatGateway" and
        .ResourceType != "AWS::EC2::EIP" and
        .ResourceType != "AWS::EC2::SecurityGroup" and
        .ResourceType != "AWS::EC2::SecurityGroupIngress" and
        .ResourceType != "AWS::EC2::SecurityGroupEgress" and
        .ResourceType != "AWS::EC2::RouteTable" and
        .ResourceType != "AWS::EC2::Route" and
        .ResourceType != "AWS::EC2::SubnetRouteTableAssociation" and
        .ResourceType != "AWS::Neptune::DBCluster" and
        .ResourceType != "AWS::Neptune::DBInstance" and
        .ResourceType != "AWS::Neptune::DBClusterParameterGroup" and
        .ResourceType != "AWS::Neptune::DBParameterGroup" and
        .ResourceType != "AWS::Neptune::DBSubnetGroup" and
        .ResourceType != "AWS::NeptuneGraph::Graph" and
        .ResourceType != "AWS::OpenSearchServerless::Collection" and
        .ResourceType != "AWS::OpenSearchServerless::AccessPolicy" and
        .ResourceType != "AWS::OpenSearchServerless::SecurityPolicy" and
        .ResourceType != "AWS::OpenSearchServerless::VpcEndpoint" and
        .ResourceType != "AWS::RDS::DBCluster" and
        .ResourceType != "AWS::RDS::DBInstance" and
        .ResourceType != "AWS::RDS::DBClusterParameterGroup" and
        .ResourceType != "AWS::RDS::DBParameterGroup" and
        .ResourceType != "AWS::RDS::DBSubnetGroup" and
        .ResourceType != "AWS::S3Vectors::VectorBucket" and
        .ResourceType != "AWS::KMS::Key" and
        .ResourceType != "AWS::SageMaker::NotebookInstance" and
        .ResourceType != "AWS::SageMaker::NotebookInstanceLifecycleConfig" and
        .ResourceType != "AWS::IAM::Role" and
        .ResourceType != "AWS::IAM::ManagedPolicy" and
        .ResourceType != "AWS::Lambda::Function" and
        .ResourceType != "Custom::CustomResource"
    ) | "\(.ResourceType)|\(.LogicalResourceId)|\(.PhysicalResourceId)|\(.ResourceStatus)"' 2>/dev/null)

    if [[ ${#other_resources[@]} -gt 0 ]]; then
        print_header "ADDITIONAL RESOURCES"
        print_table_header "Type" "Logical ID" "Status"
        for res_entry in "${other_resources[@]}"; do
            IFS='|' read -r res_type logical_id phys_id res_status <<< "$res_entry"
            ((RESOURCES_EXPECTED++)) || true
            ((RESOURCES_FOUND++)) || true
            if [[ "$res_status" == *"COMPLETE"* ]]; then
                ((RESOURCES_HEALTHY++)) || true
            else
                ((RESOURCES_UNKNOWN++)) || true
            fi
            # Shorten type for display
            local short_type="${res_type#AWS::}"
            print_table_row "$short_type" "$logical_id" "$(echo -e "$(status_color "$res_status")")"
        done
    fi

    # Custom Resources
    local custom_resources=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && custom_resources+=("$line")
    done < <(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.ResourceType=="Custom::CustomResource") | "\(.LogicalResourceId)|\(.PhysicalResourceId)|\(.ResourceStatus)"' 2>/dev/null)

    if [[ ${#custom_resources[@]} -gt 0 ]]; then
        print_subheader "Custom Resources (Lambda-backed)"
        for cr_entry in "${custom_resources[@]}"; do
            IFS='|' read -r logical_id phys_id cr_status <<< "$cr_entry"
            ((RESOURCES_EXPECTED++)) || true
            ((RESOURCES_FOUND++)) || true
            if [[ "$cr_status" == *"COMPLETE"* ]]; then
                ((RESOURCES_HEALTHY++)) || true
            fi
            print_status "$logical_id" "$cr_status" "ok"
        done
    fi
}

###############################################################################
# SUMMARY
###############################################################################

print_summary() {
    print_header "AUDIT SUMMARY"

    # Resource count from stack
    local total_stack_resources
    total_stack_resources=$(echo "$STACK_RESOURCES" | jq '.StackResourceSummaries | length' 2>/dev/null || echo "0")

    echo ""
    echo -e "  ${BOLD}Resource Overview${RESET}"
    echo -e "  ─────────────────────────────────────────"
    printf "    %-30s %s\n" "Stack Resources (total):" "$total_stack_resources"
    printf "    %-30s ${GREEN}%s${RESET}\n" "Resources Verified:" "$RESOURCES_FOUND"
    printf "    %-30s ${GREEN}%s${RESET}\n" "Healthy:" "$RESOURCES_HEALTHY"
    printf "    %-30s ${RED}%s${RESET}\n" "Unhealthy/Missing:" "$RESOURCES_UNHEALTHY"
    printf "    %-30s ${YELLOW}%s${RESET}\n" "Unknown:" "$RESOURCES_UNKNOWN"
    echo ""

    # Health percentage
    local health_pct=0
    if [[ $RESOURCES_FOUND -gt 0 ]]; then
        health_pct=$(( (RESOURCES_HEALTHY * 100) / RESOURCES_FOUND ))
    fi

    echo -e "  ${BOLD}Health Score${RESET}"
    echo -e "  ─────────────────────────────────────────"
    if [[ $health_pct -ge 90 ]]; then
        echo -e "    ${GREEN}██████████ ${health_pct}% - HEALTHY${RESET}"
    elif [[ $health_pct -ge 70 ]]; then
        echo -e "    ${YELLOW}███████░░░ ${health_pct}% - DEGRADED${RESET}"
    else
        echo -e "    ${RED}████░░░░░░ ${health_pct}% - UNHEALTHY${RESET}"
    fi
    echo ""

    # Cost indicators
    echo -e "  ${BOLD}Cost Indicators (estimated hourly when running)${RESET}"
    echo -e "  ─────────────────────────────────────────"

    local has_cost_items=false

    # Check what's running
    local notebook_status
    notebook_status=$(aws_cmd sagemaker describe-notebook-instance \
        --notebook-instance-name "$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneNotebookInstance") | .PhysicalResourceId' 2>/dev/null)" \
        --query 'NotebookInstanceStatus' --output text 2>/dev/null || echo "")

    if [[ "$notebook_status" == "InService" ]]; then
        printf "    ${YELLOW}⚠${RESET} %-35s %s\n" "SageMaker Notebook:" "RUNNING (ml.m5.xlarge ~\$0.23/hr)"
        has_cost_items=true
    fi

    # Neptune Analytics
    local graph_id
    graph_id=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="Graph") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$graph_id" ]]; then
        printf "    ${YELLOW}⚠${RESET} %-35s %s\n" "Neptune Analytics Graph:" "Provisioned memory charges apply"
        has_cost_items=true
    fi

    # Neptune DB
    local neptune_cluster
    neptune_cluster=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NeptuneDBCluster") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$neptune_cluster" ]]; then
        printf "    ${YELLOW}⚠${RESET} %-35s %s\n" "Neptune DB Cluster:" "Instance/serverless charges apply"
        has_cost_items=true
    fi

    # OpenSearch
    local opensearch_col
    opensearch_col=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="OpenSearchServerless") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$opensearch_col" ]]; then
        printf "    ${YELLOW}⚠${RESET} %-35s %s\n" "OpenSearch Serverless:" "Min 4 OCUs (~\$0.96/hr)"
        has_cost_items=true
    fi

    # Aurora
    local aurora_cluster
    aurora_cluster=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="PostgresCluster") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$aurora_cluster" ]]; then
        printf "    ${YELLOW}⚠${RESET} %-35s %s\n" "Aurora PostgreSQL:" "Serverless/instance charges apply"
        has_cost_items=true
    fi

    # NAT Gateway
    local nat_gw
    nat_gw=$(echo "$STACK_RESOURCES" | jq -r '.StackResourceSummaries[]? | select(.LogicalResourceId=="NATGW") | .PhysicalResourceId' 2>/dev/null || echo "")
    if [[ -n "$nat_gw" ]]; then
        printf "    ${YELLOW}⚠${RESET} %-35s %s\n" "NAT Gateway:" "~\$0.045/hr + data"
        has_cost_items=true
    fi

    if [[ "$has_cost_items" == "false" ]]; then
        printf "    ${GREEN}✓${RESET} No active cost-incurring resources detected\n"
    fi

    echo ""
    echo -e "  ${DIM}Audit completed at: $(date -u '+%Y-%m-%d %H:%M:%S UTC')${RESET}"
    echo -e "  ${DIM}Stack: ${STACK_NAME} | Region: ${REGION}${RESET}"
    echo ""
}

###############################################################################
# JSON OUTPUT
###############################################################################

output_json() {
    local total_stack_resources
    total_stack_resources=$(echo "$STACK_RESOURCES" | jq '.StackResourceSummaries | length' 2>/dev/null || echo "0")

    local health_pct=0
    if [[ $RESOURCES_FOUND -gt 0 ]]; then
        health_pct=$(( (RESOURCES_HEALTHY * 100) / RESOURCES_FOUND ))
    fi

    # Build full JSON output
    cat <<EOF
{
  "audit_metadata": {
    "timestamp": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
    "stack_name": "$STACK_NAME",
    "region": "$REGION",
    "version": "$VERSION"
  },
  "summary": {
    "total_stack_resources": $total_stack_resources,
    "resources_verified": $RESOURCES_FOUND,
    "resources_healthy": $RESOURCES_HEALTHY,
    "resources_unhealthy": $RESOURCES_UNHEALTHY,
    "resources_unknown": $RESOURCES_UNKNOWN,
    "health_percentage": $health_pct
  },
  "stack_resources": $(echo "$STACK_RESOURCES" | jq '.StackResourceSummaries // []'),
  "stack_info": ${JSON_SECTION_CLOUDFORMATION:-"{}"}
}
EOF
}

###############################################################################
# MAIN EXECUTION
###############################################################################

main() {
    parse_args "$@"

    # Verify AWS CLI is available
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}Error: AWS CLI not found. Please install it first.${RESET}" >&2
        exit 1
    fi

    # Verify jq is available
    if ! command -v jq &> /dev/null; then
        echo -e "${RED}Error: jq not found. Please install it (brew install jq / apt install jq).${RESET}" >&2
        exit 1
    fi

    # Verify AWS credentials
    if ! aws_cmd sts get-caller-identity &> /dev/null; then
        echo -e "${RED}Error: Unable to authenticate with AWS. Check your credentials/profile.${RESET}" >&2
        exit 1
    fi

    if [[ "$JSON_OUTPUT" != "true" ]]; then
        echo ""
        echo -e "${BOLD}${MAGENTA}╔══════════════════════════════════════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}${MAGENTA}║           GraphRAG Toolkit - Deployment Audit Script v${VERSION}              ║${RESET}"
        echo -e "${BOLD}${MAGENTA}╚══════════════════════════════════════════════════════════════════════════════╝${RESET}"
        echo ""
        local mode_label="Stack"
        [[ "$MANUAL_MODE" == "true" ]] && mode_label="Manual Scan"
        echo -e "  ${DIM}Region: ${REGION} | Profile: ${PROFILE:-default} | Mode: ${mode_label}${RESET}"
    fi

    if [[ "$MANUAL_MODE" == "true" ]]; then
        # Manual mode: scan AWS directly without needing a CloudFormation stack
        discover_resources_manual

        # Skip CloudFormation audit, go straight to resource audits
        audit_networking
        audit_neptune_db
        audit_neptune_analytics
        audit_opensearch
        audit_aurora_postgres
        audit_s3_vectors
        audit_sagemaker
        audit_iam
        audit_lambda
        audit_additional_resources
    else
        # Stack mode: detect or use provided stack name
        detect_stack

        # Run all audit sections
        audit_cloudformation
        audit_networking
        audit_neptune_db
        audit_neptune_analytics
        audit_opensearch
        audit_aurora_postgres
        audit_s3_vectors
        audit_sagemaker
        audit_iam
        audit_lambda
        audit_additional_resources
    fi

    # Output results
    if [[ "$JSON_OUTPUT" == "true" ]]; then
        output_json
    else
        print_summary
    fi
}

# Run main with all arguments
main "$@"
